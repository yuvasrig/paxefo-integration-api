// Package ingest orchestrates the ingest pipeline (DESIGN.md section 4.1, sync mode): tenant routing,
// raw save (transaction 1), parse + rules + transform, then normalized rows and outcome (transaction 2).
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/yuvasrig/paxefo-integration-api/internal/doc"
	"github.com/yuvasrig/paxefo-integration-api/internal/paxafe"
	"github.com/yuvasrig/paxefo-integration-api/internal/provider"
	"github.com/yuvasrig/paxefo-integration-api/internal/rules"
	"github.com/yuvasrig/paxefo-integration-api/internal/store"
)

// CommonPool is the reserved tenant for data from provider accounts nobody has claimed (F3-R9).
const CommonPool = "common-pool"

// ErrNotReplayable is returned when an event already succeeded (F7-R4).
var ErrNotReplayable = errors.New("event is already processed; nothing to replay")

// Store is the part of the store the pipeline needs (a real boundary, faked in tests).
type Store interface {
	SaveRaw(ctx context.Context, ev store.RawEvent) error
	ResolveDevice(ctx context.Context, tenantID, name string) (imei string, ok bool, err error)
	Commit(ctx context.Context, in store.CommitInput) (store.CommitResult, error)
	RecordFailure(ctx context.Context, tenantID, submittedBy, rawEventID, message string) error
	GetEvent(ctx context.Context, viewerTenant, id string) (*store.EventRecord, error)
	ListReadings(ctx context.Context, tenantID string, q store.ReadingsQuery) (store.ReadingsPage, error)
	TenantForAccount(ctx context.Context, providerName string, accountID int64) (tenantID string, ok bool, err error)
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

// Request is one incoming payload.
type Request struct {
	SubmittedBy string // tenant of the API key
	RequestID   string
	Body        []byte
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
}

// Ingest handles one webhook payload. An error means the raw payload could not be saved: nothing was
// acknowledged and the sender should retry. Once the raw payload is saved, failures are reported in the
// Response (status failed, HTTP 500) and can be replayed.
func (s *Service) Ingest(ctx context.Context, req Request) (Response, error) {
	owner, preFlags, mismatch, err := s.route(ctx, req.SubmittedBy, req.Body)
	if err != nil {
		return Response{}, err
	}

	id := store.NewID("evt_")
	t0 := time.Now()
	err = s.Store.SaveRaw(ctx, store.RawEvent{
		ID: id, TenantID: owner, SubmittedBy: req.SubmittedBy, Provider: s.Provider.Name(),
		Raw: req.Body, Hash: doc.CanonicalHash(req.Body), ReceivedAt: s.Now(), RequestID: req.RequestID,
	})
	s.stage("raw_save", t0)
	if err != nil {
		return Response{}, fmt.Errorf("ingest: save raw event: %w", err)
	}
	return s.process(ctx, run{eventID: id, requestID: req.RequestID, owner: owner, submittedBy: req.SubmittedBy, body: req.Body, preFlags: preFlags, mismatch: mismatch}), nil
}

// Replay re-runs a failed, rejected, duplicate, conflict or never-finished event from its stored raw payload (F7-R2).
// It is idempotent: readings are unique per key and the raw payload is never changed.
func (s *Service) Replay(ctx context.Context, viewerTenant, eventID, requestID string) (Response, error) {
	ev, err := s.Store.GetEvent(ctx, viewerTenant, eventID)
	if err != nil {
		return Response{}, err
	}
	if ev.HasOutcome && (ev.Status == provider.StatusAccepted || ev.Status == provider.StatusFlagged) {
		return Response{}, ErrNotReplayable
	}
	_, preFlags, mismatch, err := s.route(ctx, ev.SubmittedBy, ev.Raw)
	if err != nil {
		return Response{}, err
	}
	return s.process(ctx, run{eventID: ev.ID, requestID: requestID, owner: ev.TenantID, submittedBy: ev.SubmittedBy, body: ev.Raw, preFlags: preFlags, mismatch: mismatch}), nil
}

// Event returns an event with its outcome for the tenant that owns or submitted it.
func (s *Service) Event(ctx context.Context, viewerTenant, id string) (*store.EventRecord, error) {
	return s.Store.GetEvent(ctx, viewerTenant, id)
}

// Readings lists a device's readings for a tenant.
func (s *Service) Readings(ctx context.Context, tenantID string, q store.ReadingsQuery) (store.ReadingsPage, error) {
	return s.Store.ListReadings(ctx, tenantID, q)
}

type run struct {
	eventID, requestID, owner, submittedBy string
	body                                   []byte
	preFlags                               []rules.Flag
	mismatch                               bool
}

// route decides which tenant owns the payload (DESIGN.md section 4.1 step 1a). The API key's tenant is the
// default; AccountId can only route to common-pool, never into another customer's tenant.
func (s *Service) route(ctx context.Context, submitter string, body []byte) (owner string, flags []rules.Flag, mismatch bool, err error) {
	d, perr := doc.Parse(body)
	if perr != nil {
		return submitter, nil, false, nil // invalid JSON is stored under the sender and rejected later
	}
	acct, ok := accountID(d)
	if !ok {
		return submitter, []rules.Flag{{Code: rules.CodeAccountIDMissing, Severity: rules.SeverityInfo, Field: "AccountId", Action: rules.ActionKept}}, false, nil
	}
	tenant, mapped, err := s.Store.TenantForAccount(ctx, s.Provider.Name(), acct)
	if err != nil {
		return "", nil, false, fmt.Errorf("ingest: route account: %w", err)
	}
	switch {
	case !mapped:
		return CommonPool, []rules.Flag{{Code: rules.CodeUnknownAccount, Severity: rules.SeverityWarn, Field: "AccountId", Value: acct, Action: rules.ActionKept}}, false, nil
	case tenant == submitter:
		return submitter, nil, false, nil
	default:
		return submitter, nil, true, nil
	}
}

// accountID reads AccountId as an integer number or numeric string; anything else counts as missing.
func accountID(d doc.Doc) (int64, bool) {
	v, _ := d.Get("AccountId")
	var text string
	switch t := v.(type) {
	case interface{ String() string }:
		text = t.String()
	case string:
		text = t
	default:
		return 0, false
	}
	n, err := strconv.ParseInt(text, 10, 64)
	return n, err == nil
}

func (s *Service) process(ctx context.Context, r run) Response {
	resp := Response{EventID: r.eventID, RequestID: r.requestID, Flags: []rules.Flag{}, Errors: []rules.Problem{}}

	t0 := time.Now()
	res, httpStatus, err := s.transform(ctx, r)
	s.stage("validate", t0)
	if err != nil {
		return s.fail(ctx, r, resp, err)
	}
	if httpStatus != 400 { // routing flags describe the payload; they do not apply to a body that is not JSON
		res.Flags = append(append([]rules.Flag(nil), r.preFlags...), res.Flags...)
	}
	res.Status = statusOf(res)

	t0 = time.Now()
	out, err := s.Store.Commit(ctx, store.CommitInput{RawEventID: r.eventID, TenantID: r.owner, SubmittedBy: r.submittedBy, Result: res})
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
	case out.Status == provider.StatusRejected:
		resp.HTTPStatus = 422
	default:
		resp.HTTPStatus = 200
	}
	if s.Observer != nil {
		s.Observer.Outcome(resp.Status, resp.Flags, resp.Errors)
	}
	return resp
}

// transform parses and transforms the payload, or builds the rejection for bad JSON and tenant mismatch.
// httpStatus is non-zero when it overrides the default (400 for invalid JSON).
func (s *Service) transform(ctx context.Context, r run) (provider.Result, int, error) {
	if r.mismatch {
		return rejected(rules.CodeTenantMismatch, "AccountId", "AccountId belongs to a different tenant than the API key"), 0, nil
	}
	d, err := s.Provider.Parse(r.body)
	if err != nil {
		if errors.Is(err, provider.ErrInvalidJSON) {
			return rejected(rules.CodeInvalidJSON, "", "request body is not a JSON object"), 400, nil
		}
		return provider.Result{}, 0, err
	}
	devs := resolver{}
	if name, ok := d["DeviceName"].(string); ok && name != "" {
		imei, found, err := s.Store.ResolveDevice(ctx, r.owner, name)
		if err != nil {
			return provider.Result{}, 0, err
		}
		if found {
			devs[name] = imei
		}
	}
	res, err := s.Provider.Transform(d, rules.Scope{Tenant: r.owner, Now: s.Now(), Devices: devs})
	return res, 0, err
}

type resolver map[string]string

func (r resolver) ResolveByName(name string) (string, bool) { v, ok := r[name]; return v, ok }

func rejected(code, field, msg string) provider.Result {
	return provider.Result{Status: provider.StatusRejected, Errors: []rules.Problem{{Code: code, Field: field, Message: msg}}}
}

// statusOf: rejected stays rejected; any warn or error flag makes the event flagged.
func statusOf(res provider.Result) string {
	if res.Status == provider.StatusRejected || len(res.Errors) > 0 {
		return provider.StatusRejected
	}
	for _, f := range res.Flags {
		if f.Severity == rules.SeverityWarn || f.Severity == rules.SeverityError {
			return provider.StatusFlagged
		}
	}
	return provider.StatusAccepted
}

// fail records the failure after the raw payload is saved (F7-R1). The raw event is already safe.
func (s *Service) fail(ctx context.Context, r run, resp Response, cause error) Response {
	s.log().Error("event processing failed", "event_id", r.eventID, "tenant_id", r.owner, "error", cause)
	// Use a context that survives a cancelled request so the failure is still recorded.
	if err := s.Store.RecordFailure(context.WithoutCancel(ctx), r.owner, r.submittedBy, r.eventID, cause.Error()); err != nil {
		s.log().Error("could not record failure", "event_id", r.eventID, "error", err)
	}
	resp.Status, resp.HTTPStatus = store.StatusFailed, 500
	resp.Errors = []rules.Problem{{Code: "INTERNAL", Message: "processing failed after the payload was saved; it can be replayed"}}
	if s.Observer != nil {
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
