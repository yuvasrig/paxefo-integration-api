# Expected results for the 13 provided samples

One file per sample in `assignment/sample-tive-payloads.json` (index = array position). These are the accuracy safety net: **review by hand**. The M1 golden tests and the M3 HTTP tests must match them exactly.

## Precondition (first run)
- Fresh `reviewer` tenant; Tive `AccountId` 478 mapped to it; 999 unmapped.
- Samples sent **in order 0 → 12**. #4 depends on #0 (device `A571992` becomes known); #1, #4, #8 conflict with #0.
- Default ruleset `2026-11-05.1`; accuracy thresholds 50 / 500 m.

## Re-run
Sending the same samples again to the same tenant returns `duplicate` for every sample whose first-run status was `accepted` or `flagged`; `conflict` and `rejected` samples repeat their first-run result (a conflict is again a conflict; #12 is again rejected).

## File shape
`http_status`, `status`, `owning_tenant`, `flags[]`, `errors[]`, `sensor`, `location` (the PAXAFE records as returned in the response, validated against the PAXAFE JSON Schemas), `stored` (what is persisted: for conflicts the sensor/location shown is this payload's transform, kept only in `reading_conflicts`), optional `note`.

Flag comparison is by set of `(code, field)`; order is not significant. `rule_id`, `value`, `action` are asserted too.

## Summary

| # | Sample | HTTP | status | Flags (code) |
|---|---|---|---|---|
| 0 | Standard Temperature Shipment | 200 | flagged | IDENTITY_MISMATCH |
| 1 | Device A571992 Reading | 200 | conflict | INVALID_COORDINATES, ACCOUNT_ID_MISSING, DUPLICATE_CONFLICT |
| 2 | GPS Location Shipment | 200 | accepted | — |
| 3 | Vaccine Shipment Checkpoint | 200 | flagged | SENSOR_FAULT |
| 4 | Device Name Only | 200 | conflict | DEVICE_RESOLVED_BY_NAME, ACCOUNT_ID_MISSING, DUPLICATE_CONFLICT |
| 5 | Biologics Replenishment | 200 | flagged | TIMESTAMP_SECONDS |
| 6 | Cellular Location Only | 200 | flagged | SENSOR_FAULT |
| 7 | Standard Temperature Shipment Update | 200 | flagged | COERCED_TYPE ×3, IDENTITY_MISMATCH |
| 8 | Device A571992 Follow-up Reading | 200 | conflict | INVALID_COORDINATES, ACCOUNT_ID_MISSING, DUPLICATE_CONFLICT |
| 9 | Clinical Supply Shipment | 200 | flagged | UNKNOWN_ACCOUNT (stored in `common-pool`) |
| 10 | Minimal Data Payload | 200 | accepted | ACCOUNT_ID_MISSING (info) |
| 11 | Vaccine Shipment Southbound | 200 | flagged | TIMESTAMP_MISMATCH |
| 12 | Entity and Device Name | 422 | rejected | IDENTITY_MISMATCH, ACCOUNT_ID_MISSING; error NO_DEVICE_IDENTITY |

## Not expected (designed, not built)
`IMPOSSIBLE_TRAVEL` (#2 is in San Francisco, off its Boston→Atlanta route; #3 is Richmond one hour later) and `TEMP_EXCURSION` (#0, #6, #7, #10 are outside the default 2–8 °C band).
