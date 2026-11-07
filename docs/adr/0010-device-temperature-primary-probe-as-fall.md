# ADR 0010: Device temperature primary, probe as fallback

**Status:** accepted (built) · **Source:** DESIGN.md §20, decision 8

## Decision
Device temperature primary, probe as fallback.

## Rejected
Probe primary.

## Why
The device reading is always present; the probe shows faults in the samples.

## Consequences
Fallback is flagged `TEMPERATURE_FALLBACK`; open question for PAXAFE: which is authoritative (DESIGN §21).
