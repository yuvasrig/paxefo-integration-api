# ADR 0015: OpenTelemetry, Prometheus and slog

**Status:** accepted (partly built) · **Source:** DESIGN.md §20, decision 13

## Decision
OpenTelemetry, Prometheus and slog.

## Rejected
A vendor SDK.

## Why
Portable.

## Consequences
Prometheus metrics and structured `slog` logs (no keys or payloads) are built; OpenTelemetry tracing is not.
