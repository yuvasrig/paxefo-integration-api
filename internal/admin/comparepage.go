package admin

import (
	"html/template"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/compare"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// The Compare page: what a tenant's device sent (the Tive payload as received) beside what the API produced from it
// (the PAXAFE records), as charts and as a list of what changed. Pick a tenant, a time window and a device; without a
// device it lists the devices that sent something, to pick from.

const (
	compareMaxEvents  = 1000 // events read for one comparison; more are left out and the page says so
	compareMaxChanges = 50   // events listed under "what changed"
)

type compareView struct {
	Truncated bool
	Devices   []deviceCount // shown when no device is chosen
	Summary   compare.Summary
	TempChart template.HTML
	PathChart template.HTML
	Changes   []changeRow
}

type deviceCount struct {
	Name   string
	Events int
	URL    string
}

// changeRow is one event where the API changed something, with the fields that differ.
type changeRow struct {
	ID         string
	ReceivedAt time.Time
	Status     string
	Flags      []string
	Fields     []compare.Row
}

func timeOf(ms int64) string { return time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05") }

func (c *Console) comparePage(w http.ResponseWriter, r *http.Request) {
	tenants, err := c.tenantList(r.Context())
	if err != nil {
		c.fail(w, r, "could not list tenants", err)
		return
	}
	first := ""
	if len(tenants) > 0 {
		first = tenants[0].Info.ID
	}
	pf, f, ferr := c.readFilter(r.URL.Query(), first)
	v := view{Title: "Compare", Nav: "compare", Tenants: tenants, PF: pf, Scope: pf.Tenant, Ranges: payloadRanges}
	if ferr != nil {
		v.Error = "Nothing shown: " + ferr.Error()
		c.render(w, r, "compare", v)
		return
	}
	known := false
	for _, t := range tenants {
		known = known || t.Info.ID == pf.Tenant
	}
	if !known {
		http.NotFound(w, r)
		return
	}

	f.Limit, f.After, f.Status, f.OnlyOutput = 200, nil, "", false
	var rows []store.PayloadRow
	cv := &compareView{}
	v.Compare = cv
	for len(rows) < compareMaxEvents {
		page, next, err := c.Store.ListPayloads(r.Context(), f)
		if err != nil {
			c.fail(w, r, "could not read payloads", err)
			return
		}
		rows = append(rows, page...)
		if next == nil {
			break
		}
		f.After = next
		cv.Truncated = len(rows) >= compareMaxEvents
	}

	if pf.Device == "" {
		cv.Devices = deviceCounts(rows, r.URL.Query())
		c.render(w, r, "compare", v)
		return
	}

	events := make([]compare.Event, 0, len(rows))
	points := make([]compare.Point, 0, len(rows))
	for _, row := range rows {
		events = append(events, compare.Event{Raw: row.Raw, Sensor: row.Sensor, Location: row.Location, Flags: append(row.Flags, row.Reasons...)})
		points = append(points, compare.PointOf(row.Raw, row.Sensor, row.Location))
		if len(cv.Changes) < compareMaxChanges {
			var changed []compare.Row
			for _, fr := range compare.Fields(row.Raw, row.Sensor, row.Location, nil) {
				if fr.Changed {
					changed = append(changed, fr)
				}
			}
			if len(changed) > 0 || len(row.Reasons) > 0 {
				cv.Changes = append(cv.Changes, changeRow{ID: row.ID, ReceivedAt: row.ReceivedAt, Status: row.Status, Flags: append(append([]string{}, row.Reasons...), row.Flags...), Fields: changed})
			}
		}
	}
	cv.Summary = compare.Summarize(events)
	cv.TempChart, cv.PathChart = tempChart(points), pathChart(points)
	c.render(w, r, "compare", v)
}

// deviceCounts lists the devices in the events, busiest first, each linking to its comparison.
func deviceCounts(rows []store.PayloadRow, q url.Values) []deviceCount {
	counts := map[string]int{}
	for _, row := range rows {
		if row.Device != "" {
			counts[row.Device]++
		}
	}
	var out []deviceCount
	for name, n := range counts {
		dq := cloneQuery(q)
		dq.Set("device", name)
		out = append(out, deviceCount{Name: name, Events: n, URL: "/admin/compare?" + dq.Encode()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Events != out[j].Events {
			return out[i].Events > out[j].Events
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// eventFields is the field-by-field table of one event, for its detail page.
func eventFields(ev *store.EventRecord) []compare.Row {
	return compare.Fields(ev.Raw, ev.Sensor, ev.Location, flagNotes(ev.Flags))
}

func flagNotes(flags []rules.Flag) []compare.Flag {
	out := make([]compare.Flag, 0, len(flags))
	for _, f := range flags {
		out = append(out, compare.Flag{Code: f.Code, Field: f.Field, Action: f.Action, RuleID: f.RuleID})
	}
	return out
}
