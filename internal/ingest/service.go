// Package ingest orchestrates the ingest pipeline (DESIGN.md section 4.1, sync mode): tenant routing,
// raw save (transaction 1), parse + rules + transform, then normalized rows and outcome (transaction 2).
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/paxafe"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// ErrNotReplayable is returned when an event already succeeded (F7-R4).
var ErrNotReplayable = errors.New("event is already processed; nothing to replay")

// Store is the part of the store the pipeline needs (a real boundary, faked in tests).
type Store interface {
	SaveRaw(ctx context.Context, ev store.RawEvent) error
	ResolveDevice(ctx context.Context, tenantID, name string) (imei string, ok bool, err error)
	Commit(ctx context.Context, in store.CommitInput) (store.CommitResult, error)
	RecordFailure(ctx context.Context, tenantID, rawEventID, message string) error
	GetEvent(ctx context.Context, tenantID, id string) (*store.EventRecord, error)
	ListReadings(ctx context.Context, tenantID string, q store.ReadingsQuery) (store.ReadingsPage, error)

	// Replay (operator action, see replay.go).
	ListTenants(ctx context.Context) ([]string, error)
	ListReplayEvents(ctx context.Context, f store.ReplayFilter, after store.ReplayCursor, limit int) ([]store.ReplayEvent, store.ReplayCursor, bool, error)
	InRollbackTx(ctx context.Context, tenantID string, timeout time.Duration, fn func(commit func(context.Context, store.CommitInput) (store.CommitResult, error)) error) error
	RecordReplayRun(ctx context.Context, r store.ReplayRun) error
}

// Observer receives timings and outcomes for metrics. It may be nil.
type Observer interface {
	Stage(name string, d time.Duration)
	Outcome(status string, flags []rules.Flag, errs []rules.Problem)
}

// Service runs the pipeline for one provider.
type Service struct {
	Store    Store
	Provider provider.Provider
	Now      func() time.Time
	Observer Observer
	Log      *slog.Logger
}

// Request is one incoming payload. The tenant is the API key's; the payload's AccountId never routes or validates anything.
type Request struct {
	TenantID  string
	RequestID string
	Body      []byte
}

// Response is the webhook response body (specs/001-integration-api/contracts/openapi.yaml IngestResponse).
type Response struct {
	EventID    string                 `json:"event_id"`
	RequestID  string                 `json:"request_id"`
	Status     string                 `json:"status"`
	Sensor     *paxafe.SensorRecord   `json:"sensor"`
	Location   *paxafe.LocationRecord `json:"location"`
	Flags      []rules.Flag           `json:"flags"`
	Errors     []rules.Problem        `json:"errors"`
	HTTPStatus int                    `json:"-"`
	cause      error                  // why processing failed (status failed); used by replay reports, never serialized
}

// Ingest handles one webhook payload. An error means the raw payload could not be saved: nothing was
// acknowledged and the sender should retry. Once the raw payload is saved, failures are reported in the
// Response (status failed, HTTP 500) and can be replayed.
func (s *Service) Ingest(ctx context.Context, req Request) (Response, error) {
	id := store.NewID("evt_")
	t0 := time.Now()
	err := s.Store.SaveRaw(ctx, store.RawEvent{
		ID: id, TenantID: req.TenantID, Provider: s.Provider.Name(),
		Raw: req.Body, Hash: doc.CanonicalHash(req.Body), ReceivedAt: s.Now(), RequestID: req.RequestID,
	})
	s.stage("raw_save", t0)
	if err != nil {
		return Response{}, fmt.Errorf("ingest: save raw event: %w", err)
	}
	return s.process(ctx, run{eventID: id, requestID: req.RequestID, tenant: req.TenantID, body: req.Body}), nil
}

// Replay re-runs a failed, rejected, duplicate, conflict or never-finished event from its stored raw payload (F7-R2).
// It is idempotent: readings are unique per key and the raw payload is never changed.
func (s *Service) Replay(ctx context.Context, tenantID, eventID, requestID string) (Response, error) {
	ev, err := s.Store.GetEvent(ctx, tenantID, eventID)
	if err != nil {
		return Response{}, err
	}
	// Only a failed, rejected or never-finished event can be replayed. A processed one (including a duplicate or a
	// conflict) would, under a keep_last policy, put an old version back over a newer reading.
	if ev.HasOutcome && ev.Status != provider.StatusRejected && ev.Status != store.StatusFailed {
		return Response{}, ErrNotReplayable
	}
	return s.process(ctx, run{eventID: ev.ID, requestID: requestID, tenant: ev.TenantID, body: ev.Raw}), nil
}

// Event returns an event with its outcome for the tenant that owns it.
func (s *Service) Event(ctx context.Context, tenantID, id string) (*store.EventRecord, error) {
	return s.Store.GetEvent(ctx, tenantID, id)
}

// Readings lists a device's readings for a tenant.
func (s *Service) Readings(ctx context.Context, tenantID string, q store.ReadingsQuery) (store.ReadingsPage, error) {
	return s.Store.ListReadings(ctx, tenantID, q)
}

type run struct {
	eventID, requestID, tenant string
	body                       []byte

	// Set only when an operator replays stored events (replay.go).
	replayRunID string                                                               // non-empty: the event is reprocessed (old outcome archived, own readings removed first)
	commit      func(context.Context, store.CommitInput) (store.CommitResult, error) // nil: s.Store.Commit
	dryRun      bool                                                                 // nothing may be written, not even a failure record
	quiet       bool                                                                 // do not feed live metrics
}

func (s *Service) process(ctx context.Context, r run) Response {
	resp := Response{EventID: r.eventID, RequestID: r.requestID, Flags: []rules.Flag{}, Errors: []rules.Problem{}}

	t0 := time.Now()
	res, httpStatus, err := s.transform(ctx, r)
	s.stage("validate", t0)
	if err != nil {
		return s.fail(ctx, r, resp, err)
	}

	t0 = time.Now()
	commit := r.commit
	if commit == nil {
		commit = s.Store.Commit
	}
	out, err := commit(ctx, store.CommitInput{RawEventID: r.eventID, TenantID: r.tenant, Result: res, ReplayRunID: r.replayRunID})
	s.stage("normalize_save", t0)
	if err != nil {
		return s.fail(ctx, r, resp, err)
	}

	resp.Status, resp.Sensor, resp.Location = out.Status, res.Sensor, res.Location
	resp.Flags = append(resp.Flags, out.Flags...)
	resp.Errors = append(resp.Errors, res.Errors...)
	switch {
	case httpStatus != 0:
		resp.HTTPStatus = httpStatus
	case out.Status == store.StatusConflict && out.ConflictHTTPStatus != 0:
		resp.HTTPStatus = out.ConflictHTTPStatus // the tenant's choice: 200 or 409
	case out.Status == provider.StatusRejected:
		resp.HTTPStatus = 422
	default:
		resp.HTTPStatus = 200
	}
	if s.Observer != nil && !r.quiet {
		s.Observer.Outcome(resp.Status, resp.Flags, resp.Errors)
	}
	return resp
}

// transform parses and transforms the payload, or builds the rejection for bad JSON and tenant mismatch.
// httpStatus is non-zero when it overrides the default (400 for invalid JSON).
func (s *Service) transform(ctx context.Context, r run) (provider.Result, int, error) {
	d, err := s.Provider.Parse(r.body)
	if err != nil {
		if errors.Is(err, provider.ErrInvalidJSON) {
			return rejected(rules.CodeInvalidJSON, "", "request body is not a JSON object"), 400, nil
		}
		return provider.Result{}, 0, err
	}
	devs := resolver{}
	if name, ok := d["DeviceName"].(string); ok && name != "" {
		imei, found, err := s.Store.ResolveDevice(ctx, r.tenant, name)
		if err != nil {
			return provider.Result{}, 0, err
		}
		if found {
			devs[name] = imei
		}
	}
	res, err := s.Provider.Transform(d, rules.Scope{Tenant: r.tenant, Now: s.Now(), Devices: devs})
	return res, 0, err
}

type resolver map[string]string

func (r resolver) ResolveByName(name string) (string, bool) { v, ok := r[name]; return v, ok }

func rejected(code, field, msg string) provider.Result {
	return provider.Result{Status: provider.StatusRejected, Errors: []rules.Problem{{Code: code, Field: field, Message: msg}}}
}

// fail records the failure after the raw payload is saved (F7-R1). The raw event is already safe.
func (s *Service) fail(ctx context.Context, r run, resp Response, cause error) Response {
	s.log().Error("event processing failed", "event_id", r.eventID, "tenant_id", r.tenant, "error", cause)
	// A failed replay must leave the event's existing outcome alone (its transaction rolled back), so it records
	// nothing; the operator sees the error in the replay report. A live failure is recorded so it can be replayed.
	if r.replayRunID == "" {
		// Use a context that survives a cancelled request so the failure is still recorded.
		if err := s.Store.RecordFailure(context.WithoutCancel(ctx), r.tenant, r.eventID, cause.Error()); err != nil {
			s.log().Error("could not record failure", "event_id", r.eventID, "error", err)
		}
	}
	resp.Status, resp.HTTPStatus, resp.cause = store.StatusFailed, 500, cause
	resp.Errors = []rules.Problem{{Code: "INTERNAL", Message: "processing failed after the payload was saved; it can be replayed"}}
	if s.Observer != nil && !r.quiet {
		s.Observer.Outcome(resp.Status, nil, resp.Errors)
	}
	return resp
}

func (s *Service) stage(name string, since time.Time) {
	if s.Observer != nil {
		s.Observer.Stage(name, time.Since(since))
	}
}

func (s *Service) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}
