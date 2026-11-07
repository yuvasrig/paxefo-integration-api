package admin

import (
	"fmt"
	"html/template"
	"sort"
	"strings"

	"github.com/yuvasrig/paxefo-integration-api/internal/compare"
)

// The Compare page's charts are inline SVG drawn on the server: no scripts and no inline styles (the console's
// content-security policy allows neither). What the tenant sent is the grey line; what the API produced is blue dots on
// top of it, so a match is dots on the line. A received value outside the range of the produced values (a sensor
// sentinel, an impossible latitude) is a red ring pinned to the edge of the chart, with its real value in the tooltip.

const (
	chartW, chartH = 640, 240
	padL, padR     = 56, 14
	padT, padB     = 22, 28
)

type axis struct{ lo, hi float64 }

// newAxis spans the values with a margin; a flat series still gets a visible range.
func newAxis(vals []float64) axis {
	if len(vals) == 0 {
		return axis{0, 1}
	}
	a := axis{vals[0], vals[0]}
	for _, v := range vals {
		a.lo, a.hi = min(a.lo, v), max(a.hi, v)
	}
	if a.hi-a.lo < 1e-9 {
		a.lo, a.hi = a.lo-1, a.hi+1
	}
	pad := (a.hi - a.lo) * 0.08
	return axis{a.lo - pad, a.hi + pad}
}

func (a axis) contains(v float64) bool { return v >= a.lo && v <= a.hi }

func (a axis) clamp(v float64) float64 { return min(max(v, a.lo), a.hi) }

func (a axis) pos(v, from, to float64) float64 { return from + (v-a.lo)/(a.hi-a.lo)*(to-from) }

func sortedByTime(points []compare.Point) []compare.Point {
	out := append([]compare.Point(nil), points...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].At < out[j].At })
	return out
}

// tempChart plots temperature against the reading's own time.
func tempChart(points []compare.Point) template.HTML {
	points = sortedByTime(points)
	var produced, received []float64
	for _, p := range points {
		if p.ProcTemp != nil {
			produced = append(produced, *p.ProcTemp)
		}
		if p.RecTemp != nil {
			received = append(received, *p.RecTemp)
		}
	}
	if len(produced) == 0 && len(received) == 0 {
		return ""
	}
	y := newAxis(produced)
	if len(produced) == 0 {
		y = newAxis(received)
	}
	t0, t1 := float64(points[0].At), float64(points[len(points)-1].At)
	xOf := func(at int64) float64 {
		if t1 <= t0 {
			return (padL + chartW - padR) / 2
		}
		return padL + (float64(at)-t0)/(t1-t0)*(chartW-padL-padR)
	}
	yOf := func(v float64) float64 { return y.pos(v, chartH-padB, padT) }

	var b strings.Builder
	svgOpen(&b, "Temperature in the Tive and PAXAFE payloads, over time")
	frame(&b, y, "°C", fmt.Sprintf("reading time, %s to %s UTC", clock(points[0].At), clock(points[len(points)-1].At)))
	var line []string
	for _, p := range points {
		if p.RecTemp != nil && y.contains(*p.RecTemp) {
			line = append(line, fmt.Sprintf("%.1f,%.1f", xOf(p.At), yOf(*p.RecTemp)))
		}
	}
	if len(line) > 1 {
		fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="#9aa4b2" stroke-width="2"/>`, strings.Join(line, " "))
	}
	for _, p := range points {
		switch {
		case p.RecTemp != nil && !y.contains(*p.RecTemp):
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="5" fill="none" stroke="#dc2626" stroke-width="2"><title>%s: received %g, outside the PAXAFE range, PAXAFE %s</title></circle>`,
				xOf(p.At), yOf(y.clamp(*p.RecTemp)), clock(p.At), *p.RecTemp, optional(p.ProcTemp))
		case p.ProcTemp != nil:
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="#2563eb"><title>%s: received %s, PAXAFE %g</title></circle>`,
				xOf(p.At), yOf(*p.ProcTemp), clock(p.At), optional(p.RecTemp), *p.ProcTemp)
		}
	}
	legend(&b)
	b.WriteString("</svg>")
	return template.HTML(b.String()) //nolint:gosec // built only from numbers and fixed text
}

// pathChart plots positions: longitude across, latitude up, each axis scaled to its own range.
func pathChart(points []compare.Point) template.HTML {
	points = sortedByTime(points)
	var pLats, pLons, rLats, rLons []float64
	for _, p := range points {
		if p.ProcLat != nil && p.ProcLon != nil {
			pLats, pLons = append(pLats, *p.ProcLat), append(pLons, *p.ProcLon)
		}
		if p.RecLat != nil && p.RecLon != nil {
			rLats, rLons = append(rLats, *p.RecLat), append(rLons, *p.RecLon)
		}
	}
	if len(pLats) == 0 && len(rLats) == 0 {
		return ""
	}
	x, y := newAxis(pLons), newAxis(pLats)
	if len(pLats) == 0 {
		x, y = newAxis(rLons), newAxis(rLats)
	}
	xOf := func(v float64) float64 { return x.pos(x.clamp(v), padL, chartW-padR) }
	yOf := func(v float64) float64 { return y.pos(y.clamp(v), chartH-padB, padT) }

	var b strings.Builder
	svgOpen(&b, "Route in the Tive and PAXAFE payloads, latitude against longitude")
	frame(&b, y, "lat", fmt.Sprintf("longitude %.4f to %.4f", x.lo, x.hi))
	var line []string
	for _, p := range points {
		if p.RecLat != nil && p.RecLon != nil && y.contains(*p.RecLat) && x.contains(*p.RecLon) {
			line = append(line, fmt.Sprintf("%.1f,%.1f", xOf(*p.RecLon), yOf(*p.RecLat)))
		}
	}
	if len(line) > 1 {
		fmt.Fprintf(&b, `<polyline points="%s" fill="none" stroke="#9aa4b2" stroke-width="2"/>`, strings.Join(line, " "))
	}
	for _, p := range points {
		recOut := p.RecLat != nil && p.RecLon != nil && !(y.contains(*p.RecLat) && x.contains(*p.RecLon))
		switch {
		case recOut:
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="5" fill="none" stroke="#dc2626" stroke-width="2"><title>%s: received %g, %g, outside the PAXAFE range; PAXAFE %s, %s</title></circle>`,
				xOf(*p.RecLon), yOf(*p.RecLat), clock(p.At), *p.RecLat, *p.RecLon, optional(p.ProcLat), optional(p.ProcLon))
		case p.ProcLat != nil && p.ProcLon != nil:
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="3.5" fill="#2563eb"><title>%s: PAXAFE %g, %g</title></circle>`,
				xOf(*p.ProcLon), yOf(*p.ProcLat), clock(p.At), *p.ProcLat, *p.ProcLon)
		}
	}
	legend(&b)
	b.WriteString("</svg>")
	return template.HTML(b.String()) //nolint:gosec // built only from numbers and fixed text
}

func svgOpen(b *strings.Builder, title string) {
	fmt.Fprintf(b, `<svg class="chart" viewBox="0 0 %d %d" role="img" aria-label="%s"><title>%s</title>`, chartW, chartH, title, title)
}

// frame draws three grid lines with value labels, the unit, and the x caption.
func frame(b *strings.Builder, y axis, unit, xcap string) {
	for i := 0; i <= 2; i++ {
		v := y.lo + (y.hi-y.lo)*float64(i)/2
		py := y.pos(v, chartH-padB, padT)
		fmt.Fprintf(b, `<line x1="%d" y1="%.1f" x2="%d" y2="%.1f" stroke="#e1e5ea"/>`, padL, py, chartW-padR, py)
		fmt.Fprintf(b, `<text x="%d" y="%.1f" font-size="10" text-anchor="end" fill="#667085">%.2f</text>`, padL-4, py+3, v)
	}
	fmt.Fprintf(b, `<text x="%d" y="%d" font-size="10" fill="#667085">%s</text>`, padL, chartH-8, xcap)
	fmt.Fprintf(b, `<text x="4" y="%d" font-size="10" fill="#667085">%s</text>`, padT+4, unit)
}

func legend(b *strings.Builder) {
	fmt.Fprintf(b, `<g font-size="10" fill="#667085"><line x1="%d" y1="10" x2="%d" y2="10" stroke="#9aa4b2" stroke-width="2"/><text x="%d" y="13">Tive (received)</text>`+
		`<circle cx="%d" cy="10" r="3.5" fill="#2563eb"/><text x="%d" y="13">PAXAFE payload</text>`+
		`<circle cx="%d" cy="10" r="4.5" fill="none" stroke="#dc2626" stroke-width="2"/><text x="%d" y="13">Tive out of range</text></g>`,
		chartW-400, chartW-380, chartW-376, chartW-278, chartW-270, chartW-170, chartW-160)
}

func clock(ms int64) string {
	if ms <= 0 {
		return "?"
	}
	return timeOf(ms)
}

func optional(v *float64) string {
	if v == nil {
		return "nothing"
	}
	return fmt.Sprintf("%g", *v)
}
