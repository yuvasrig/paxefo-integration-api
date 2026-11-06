// Package observability holds the Prometheus metrics and the structured logger (DESIGN.md section 10).
package observability

import (
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
)

// NewLogger returns a JSON logger at the given level (debug, info, warn or error).
func NewLogger(level string, w io.Writer) *slog.Logger {
	var l slog.Level
	_ = l.UnmarshalText([]byte(level))
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l}))
}

// Metrics owns a private registry, so tests and instances never share global state.
type Metrics struct {
	registry   *prometheus.Registry
	requests   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	stages     *prometheus.HistogramVec
	flags      *prometheus.CounterVec
	rejects    *prometheus.CounterVec
	events     *prometheus.CounterVec
	reloads    *prometheus.CounterVec
	rulesGauge *prometheus.GaugeVec
}

// NewMetrics registers the RED metrics, per-stage latency, quality counters, Go runtime and (if pool is not nil)
// database pool statistics.
func NewMetrics(pool func() *pgxpool.Stat) *Metrics {
	m := &Metrics{
		registry:   prometheus.NewRegistry(),
		requests:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "http_requests_total", Help: "HTTP requests by route and status."}, []string{"route", "status"}),
		duration:   prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "http_request_duration_seconds", Help: "HTTP request latency by route.", Buckets: prometheus.DefBuckets}, []string{"route"}),
		stages:     prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "ingest_stage_duration_seconds", Help: "Ingest pipeline stage latency.", Buckets: prometheus.DefBuckets}, []string{"stage"}),
		flags:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "quality_flags_total", Help: "Quality flags raised, by code."}, []string{"code"}),
		rejects:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rejects_total", Help: "Rejected or failed payloads, by reason."}, []string{"reason"}),
		events:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "events_total", Help: "Processed events by final status."}, []string{"status"}),
		reloads:    prometheus.NewCounterVec(prometheus.CounterOpts{Name: "rules_reloads_total", Help: "Runtime ruleset reload attempts by result (changed, invalid, error)."}, []string{"result"}),
		rulesGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "rules_active_info", Help: "The ruleset in force (value 1), by version and source."}, []string{"version", "source"}),
	}
	m.registry.MustRegister(m.requests, m.duration, m.stages, m.flags, m.rejects, m.events, m.reloads, m.rulesGauge,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if pool != nil {
		m.registry.MustRegister(&poolCollector{stat: pool})
	}
	return m
}

// Handler serves /metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the registry for tests.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ObserveRequest records one HTTP request; route is the mux pattern, so cardinality stays bounded.
func (m *Metrics) ObserveRequest(route string, status int, d time.Duration) {
	m.requests.WithLabelValues(route, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(route).Observe(d.Seconds())
}

// Stage implements ingest.Observer.
func (m *Metrics) Stage(name string, d time.Duration) {
	m.stages.WithLabelValues(name).Observe(d.Seconds())
}

// Outcome implements ingest.Observer.
func (m *Metrics) Outcome(status string, flags []rules.Flag, errs []rules.Problem) {
	m.events.WithLabelValues(status).Inc()
	for _, f := range flags {
		m.flags.WithLabelValues(f.Code).Inc()
	}
	for _, e := range errs {
		m.rejects.WithLabelValues(e.Code).Inc()
	}
}

// RulesReload implements rulesync.Observer.
func (m *Metrics) RulesReload(result string) { m.reloads.WithLabelValues(result).Inc() }

// RulesActive implements rulesync.Observer: it records which ruleset is in force.
func (m *Metrics) RulesActive(version, source string) {
	m.rulesGauge.Reset()
	m.rulesGauge.WithLabelValues(version, source).Set(1)
}

// poolCollector exposes pgxpool statistics, including time spent waiting for a connection.
type poolCollector struct{ stat func() *pgxpool.Stat }

var (
	descAcquired = prometheus.NewDesc("db_pool_acquired_conns", "Connections currently in use.", nil, nil)
	descIdle     = prometheus.NewDesc("db_pool_idle_conns", "Idle connections.", nil, nil)
	descTotal    = prometheus.NewDesc("db_pool_total_conns", "Open connections.", nil, nil)
	descMax      = prometheus.NewDesc("db_pool_max_conns", "Configured maximum connections.", nil, nil)
	descWait     = prometheus.NewDesc("db_pool_acquire_wait_seconds_total", "Total time spent waiting to acquire a connection.", nil, nil)
	descAcquires = prometheus.NewDesc("db_pool_acquires_total", "Total connection acquisitions.", nil, nil)
)

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{descAcquired, descIdle, descTotal, descMax, descWait, descAcquires} {
		ch <- d
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.stat()
	ch <- prometheus.MustNewConstMetric(descAcquired, prometheus.GaugeValue, float64(s.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(descIdle, prometheus.GaugeValue, float64(s.IdleConns()))
	ch <- prometheus.MustNewConstMetric(descTotal, prometheus.GaugeValue, float64(s.TotalConns()))
	ch <- prometheus.MustNewConstMetric(descMax, prometheus.GaugeValue, float64(s.MaxConns()))
	ch <- prometheus.MustNewConstMetric(descWait, prometheus.CounterValue, s.AcquireDuration().Seconds())
	ch <- prometheus.MustNewConstMetric(descAcquires, prometheus.CounterValue, float64(s.AcquireCount()))
}
