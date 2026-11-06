# Expected results for the 13 provided samples

One file per sample in `assignment/sample-tive-payloads.json` (index = array position). These are the **golden outputs**: they win over DESIGN.md if the two disagree. The M1 golden test and the HTTP end-to-end test must match them exactly.

## Precondition (first run)
- An empty database and one tenant; samples are sent **in order 0 → 12** with that tenant's API key.
- Default ruleset `2026-11-06.1`; accuracy thresholds 50 / 500 m; temperature band 2-8 C.
- The tenant comes from the API key only. `AccountId` is stored as metadata and never checked; `EntryTimeUtc` is ignored.
- `status` is `flagged` if and only if the event carries any flag.

## Re-run
Sending the same samples again to the same tenant returns `duplicate` for every sample whose first-run status was `accepted` or `flagged`; `conflict` and `rejected` samples repeat their first-run result.

## File shape
`http_status`, `status`, `owning_tenant`, `flags[]`, `errors[]`, `sensor`, `location` (the PAXAFE records as returned in the response, validated against the PAXAFE JSON Schemas), `stored` (what is persisted: for conflicts the sensor/location shown is this payload's transform, kept only in `reading_conflicts`), optional `note`.

Flags are compared as a set; order is not significant. `rule_id`, `value`, `new_value` and `action` are asserted too.

## Summary

| # | Sample | HTTP | status | Flags (code) |
|---|---|---|---|---|
| 0 | Standard Temperature Shipment | 200 | flagged | IDENTITY_MISMATCH, TEMP_EXCURSION |
| 1 | Device A571992 Reading | 200 | conflict | INVALID_COORDINATES, DUPLICATE_CONFLICT |
| 2 | GPS Location Shipment | 200 | accepted | — |
| 3 | Vaccine Shipment Checkpoint | 200 | flagged | SENSOR_FAULT |
| 4 | Device Name Only | 200 | conflict | DEVICE_RESOLVED_BY_NAME, DUPLICATE_CONFLICT |
| 5 | Biologics Replenishment | 200 | flagged | TIMESTAMP_SECONDS |
| 6 | Cellular Location Only | 200 | flagged | SENSOR_FAULT |
| 7 | Standard Temperature Shipment Update | 200 | flagged | COERCED_TYPE ×3, IDENTITY_MISMATCH, TEMP_EXCURSION |
| 8 | Device A571992 Follow-up Reading | 200 | conflict | INVALID_COORDINATES, DUPLICATE_CONFLICT |
| 9 | Clinical Supply Shipment | 200 | accepted | — (AccountId 999 is just metadata) |
| 10 | Minimal Data Payload | 200 | accepted | — |
| 11 | Vaccine Shipment Southbound | 200 | accepted | — (EntryTimeUtc is 5 h off and ignored) |
| 12 | Entity and Device Name | 422 | rejected | IDENTITY_MISMATCH; error NO_DEVICE_IDENTITY |

## Notes
- `TEMP_EXCURSION` fires on #0 (10.08 C) and #7 (10.5 C): both carry a shipment. #6 is -18.5 C but has no shipment, so no flag.
- `IMPOSSIBLE_TRAVEL` (#2 is in San Francisco, off its Boston→Atlanta route; #3 is Richmond an hour later) is designed, not built.
