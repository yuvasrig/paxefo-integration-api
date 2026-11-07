package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// The Payloads page: the PAXAFE records (sensor and location) the API produced from what each tenant sent, with the
// Tive payload as received one click away. Filter by tenant and time (when it was received, or the reading's own
// time), device, status, and whether it produced a PAXAFE record. All times are UTC, like the records themselves.

const payloadTimeLayout = "2006-01-02T15:04" // what <input type="datetime-local"> sends

// payloadFilter is the filter as the form shows it (text, so a mistake can be shown back to the person who made it).
type payloadFilter struct {
	Tenant    string
	TimeField string // received or event
	Range     string // 15m, 1h, 6h, 24h, 7d, all or custom
	From, To  string // custom range, UTC
	Device    string
	Status    string
	Output    bool
	Limit     int
}

var payloadRanges = []struct{ Value, Label string }{
	{"15m", "last 15 minutes"}, {"1h", "last hour"}, {"6h", "last 6 hours"}, {"24h", "last 24 hours"},
	{"7d", "last 7 days"}, {"all", "all time"}, {"custom", "custom range (UTC)"},
}

var payloadStatuses = []string{"accepted", "flagged", "rejected", "duplicate", "conflict", "pending", "dead", "failed"}

var rangeLengths = map[string]time.Duration{"15m": 15 * time.Minute, "1h": time.Hour, "6h": 6 * time.Hour, "24h": 24 * time.Hour, "7d": 7 * 24 * time.Hour}

// payloadView is one row prepared for display.
type payloadView struct {
	store.PayloadRow
	Summary      string
	SensorJSON   string
	LocationJSON string
}

// readFilter reads and checks the query. defaultTenant is used when none is given.
func (c *Console) readFilter(q url.Values, defaultTenant string) (payloadFilter, store.PayloadFilter, error) {
	pf := payloadFilter{
		Tenant: q.Get("tenant"), TimeField: q.Get("time"), Range: q.Get("range"), From: strings.TrimSpace(q.Get("from")), To: strings.TrimSpace(q.Get("to")),
		Device: strings.TrimSpace(q.Get("device")), Status: q.Get("status"), Output: q.Get("output") == "1",
	}
	if pf.Tenant == "" {
		pf.Tenant = defaultTenant
	}
	if pf.TimeField != store.TimeEvent {
		pf.TimeField = store.TimeReceived
	}
	if _, known := rangeLengths[pf.Range]; !known && pf.Range != "all" && pf.Range != "custom" {
		pf.Range = "24h"
	}
	pf.Limit, _ = strconv.Atoi(q.Get("limit"))
	if pf.Limit != 50 && pf.Limit != 100 && pf.Limit != 200 {
		pf.Limit = 25
	}
	f := store.PayloadFilter{TenantID: pf.Tenant, TimeField: pf.TimeField, Device: pf.Device, Status: pf.Status, OnlyOutput: pf.Output, Limit: pf.Limit}
	switch pf.Range {
	case "all":
	case "custom":
		var err error
		if f.From, err = parseUTC(pf.From); err != nil {
			return pf, f, fmt.Errorf("from: %w", err)
		}
		if f.To, err = parseUTC(pf.To); err != nil {
			return pf, f, fmt.Errorf("to: %w", err)
		}
		if !f.From.IsZero() && !f.To.IsZero() && !f.To.After(f.From) {
			return pf, f, errors.New("the end of the range must be after its start")
		}
	default:
		f.From = c.Now().Add(-rangeLengths[pf.Range])
	}
	if at, id := q.Get("after_at"), q.Get("after_id"); at != "" && id != "" {
		t, err := time.Parse(time.RFC3339Nano, at)
		if err != nil {
			return pf, f, errors.New("that page link is not valid")
		}
		f.After = &store.PayloadCursor{ReceivedAt: t, ID: id}
	}
	return pf, f, nil
}

// parseUTC reads a datetime-local value as UTC; empty leaves that end of the range open.
func parseUTC(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	for _, layout := range []string{payloadTimeLayout, "2006-01-02T15:04:05"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("%q is not a date and time like 2026-11-06T14:30", s)
}

// firstTenant is the tenant shown when none is chosen.
func (c *Console) tenantList(ctx context.Context) ([]TenantView, error) {
	infos, err := c.Store.ListTenantInfo(ctx)
	if err != nil {
		return nil, err
	}
	var out []TenantView
	for _, i := range infos {
		out = append(out, TenantView{Info: i})
	}
	return out, nil
}

// summarize says in a line what the PAXAFE records hold, for the table.
func summarize(sensor, location json.RawMessage) string {
	var parts []string
	var s struct {
		Temperature *float64 `json:"temperature"`
		Humidity    *float64 `json:"humidity"`
	}
	if json.Unmarshal(sensor, &s) == nil && sensor != nil {
		if s.Temperature != nil {
			parts = append(parts, strconv.FormatFloat(*s.Temperature, 'f', -1, 64)+" °C")
		}
		if s.Humidity != nil {
			parts = append(parts, strconv.FormatFloat(*s.Humidity, 'f', -1, 64)+" % RH")
		}
	}
	var l struct {
		Latitude  *float64 `json:"latitude"`
		Longitude *float64 `json:"longitude"`
	}
	if json.Unmarshal(location, &l) == nil && location != nil && l.Latitude != nil && l.Longitude != nil {
		parts = append(parts, strconv.FormatFloat(*l.Latitude, 'f', 5, 64)+", "+strconv.FormatFloat(*l.Longitude, 'f', 5, 64))
	}
	return strings.Join(parts, " · ")
}

func pretty(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if json.Indent(&buf, raw, "", "  ") != nil {
		return string(raw) // not JSON (the raw payload is kept even then): show it as it is
	}
	return buf.String()
}

func (c *Console) payloadsPage(w http.ResponseWriter, r *http.Request) {
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
	v := view{Title: "Payloads", Nav: "payloads", Tenants: tenants, PF: pf, Scope: pf.Tenant, Ranges: payloadRanges, StatusOptions: payloadStatuses, Limits: []int{25, 50, 100, 200}}
	if ferr != nil {
		v.Error = "Nothing shown: " + ferr.Error()
		c.render(w, r, "payloads", v)
		return
	}
	if pf.Tenant != "" {
		known := false
		for _, t := range tenants {
			known = known || t.Info.ID == pf.Tenant
		}
		if !known {
			http.NotFound(w, r)
			return
		}
		rows, next, err := c.Store.ListPayloads(r.Context(), f)
		if err != nil {
			c.fail(w, r, "could not read payloads", err)
			return
		}
		for _, row := range rows {
			v.Payloads = append(v.Payloads, payloadView{PayloadRow: row, Summary: summarize(row.Sensor, row.Location), SensorJSON: pretty(row.Sensor), LocationJSON: pretty(row.Location)})
		}
		q := r.URL.Query()
		q.Set("tenant", pf.Tenant)
		if next != nil {
			nq := cloneQuery(q)
			nq.Set("after_at", next.ReceivedAt.UTC().Format(time.RFC3339Nano))
			nq.Set("after_id", next.ID)
			v.NextURL = "/admin/payloads?" + nq.Encode()
		}
		jq := cloneQuery(q)
		jq.Del("after_at")
		jq.Del("after_id")
		v.JSONURL = "/admin/payloads.json?" + jq.Encode()
	}
	c.render(w, r, "payloads", v)
}

func cloneQuery(q url.Values) url.Values {
	out := url.Values{}
	for k, vs := range q {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// payloadsJSON is the same filtered list as JSON: for each event the PAXAFE sensor and location records.
func (c *Console) payloadsJSON(w http.ResponseWriter, r *http.Request) {
	pf, f, err := c.readFilter(r.URL.Query(), "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if pf.Tenant == "" {
		http.Error(w, "tenant is required", http.StatusBadRequest)
		return
	}
	if ok, err := c.Store.TenantExists(r.Context(), pf.Tenant); err != nil || !ok {
		http.NotFound(w, r)
		return
	}
	rows, _, err := c.Store.ListPayloads(r.Context(), f)
	if err != nil {
		c.fail(w, r, "could not read payloads", err)
		return
	}
	type item struct {
		EventID    string          `json:"event_id"`
		ReceivedAt time.Time       `json:"received_at"`
		Status     string          `json:"status"`
		Flags      []string        `json:"flags"`
		Sensor     json.RawMessage `json:"sensor"`
		Location   json.RawMessage `json:"location"`
	}
	out := make([]item, 0, len(rows))
	for _, row := range rows {
		it := item{EventID: row.ID, ReceivedAt: row.ReceivedAt.UTC(), Status: row.Status, Flags: row.Flags, Sensor: row.Sensor, Location: row.Location}
		if it.Flags == nil {
			it.Flags = []string{}
		}
		if it.Sensor == nil {
			it.Sensor = json.RawMessage("null")
		}
		if it.Location == nil {
			it.Location = json.RawMessage("null")
		}
		out = append(out, it)
	}
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

// payloadDetail shows one event: the PAXAFE payload it produced beside the Tive payload as received, and why.
func (c *Console) payloadDetail(w http.ResponseWriter, r *http.Request) {
	tenant, id := r.URL.Query().Get("tenant"), r.PathValue("id")
	if tenant == "" {
		http.NotFound(w, r)
		return
	}
	ev, err := c.Store.GetEvent(r.Context(), tenant, id)
	if errors.Is(err, store.ErrNotFound) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		c.fail(w, r, "could not read the event", err)
		return
	}
	c.render(w, r, "payload", view{
		Title: "Payload " + id, Nav: "payloads", Scope: tenant, PayloadEvent: ev,
		SensorJSON: pretty(ev.Sensor), LocationJSON: pretty(ev.Location), RawJSON: pretty(ev.Raw), FieldRows: eventFields(ev),
	})
}
