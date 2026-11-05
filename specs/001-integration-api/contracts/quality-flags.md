# Quality flags, reject codes, and status

Source of truth: DESIGN.md §5. A flag is `{code, field, value, action}` plus, for fixes, `original`/`new` values and the `rule_id` that produced it.

## Event status (one per event)

Precedence: `rejected` > `conflict` > `duplicate` > `flagged` > `accepted`.

- `flagged` = at least one `warn` or `error` flag.
- `info` flags alone keep the event `accepted`.
- All flags are returned whatever the status.
- Other stored statuses: `failed` (error after raw save), `dead` (async only, M8a), `queued` (async 202 response only).

## Reject codes (HTTP 422; 400 for `INVALID_JSON`)

| Code | When |
|---|---|
| `INVALID_JSON` | Body is not valid JSON (400) |
| `NO_DEVICE_IDENTITY` | `DeviceId` missing/not 15 digits **and** `DeviceName` not found in `devices` for the tenant. `EntityName` is never used for resolution |
| `NO_TIMESTAMP` | `EntryTimeEpoch` missing, not an integer, or ≤ 0 |
| `TIMESTAMP_FUTURE` | Timestamp more than 5 min ahead of server time (limit is config) |
| `NO_USABLE_DATA` | No usable temperature **and** no valid coordinates |
| `TENANT_MISMATCH` | `AccountId` maps to a known tenant other than the API key's |

Flags raised before the rejection are still returned. A rejected event is stored in `raw_events` with outcome `rejected`.

## Flag catalog

| Code | Severity | Trigger | Action | Produced by |
|---|---|---|---|---|
| `INVALID_COORDINATES` | error | Latitude ∉ [-90, 90] or longitude ∉ [-180, 180] | Location record skipped | rules `LAT-RANGE`, `LON-RANGE` |
| `SENSOR_FAULT` | warn | `Temperature.Celsius` or `ProbeTemperature.Celsius` ∈ {-32768, 327.67, 32767} | Field → null | rules `TEMP-SENTINEL`, `PROBE-SENTINEL` |
| `OUT_OF_RANGE` | warn | Humidity ∉ [0, 100], lux < 0, battery ∉ [0, 100] | Field → null | rules `HUMIDITY-RANGE`, `LUX-NEGATIVE`, `BATTERY-RANGE` |
| `COERCED_TYPE` | info | Number sent as a string | Parsed, kept | rule `NUMERIC-STRING` |
| `TIMESTAMP_SECONDS` | warn | `EntryTimeEpoch` < 1e12 | × 1000 | rule `TS-SECONDS` |
| `TIMESTAMP_MISMATCH` | warn | `EntryTimeEpoch` and `EntryTimeUtc` differ by more than 1 s | Epoch kept | rule `TS-UTC-MISMATCH` |
| `DEVICE_RESOLVED_BY_NAME` | info | No valid IMEI; found by `DeviceName` | IMEI filled | rule `NAME-RESOLVED` |
| `IDENTITY_MISMATCH` | warn | `EntityName` ≠ `DeviceName`, or `Shipment.DeviceId` ≠ `DeviceId` | Kept as sent | rules `ENTITY-NAME-MISMATCH`, `SHIPMENT-DEVICE-MISMATCH` |
| `TEMPERATURE_FALLBACK` | info | Device temperature missing/faulty, probe used | `temperature_source=probe` | transform |
| `TEMPERATURE_CONVERTED` | info | `Celsius` null, `Fahrenheit` present | Celsius = (F−32)×5/9 | rule `TEMP-F-ONLY` |
| `UNKNOWN_ACCOUNT` | warn | `AccountId` not mapped to any tenant | Stored under `common-pool` | ingest |
| `ACCOUNT_ID_MISSING` | info | No `AccountId` | Key's tenant used | ingest |
| `DUPLICATE_CONFLICT` | warn | Same `(tenant, imei, ts)`, different content | First kept; conflict stored | store |
| `TEMP_EXCURSION` | warn | Outside shipment band (default 2–8 °C) | Flag only | domain — *designed, not built (M0–M7)* |
| `IMPOSSIBLE_TRAVEL` | warn | Speed vs time-nearest neighbours > 1000 km/h | Flag only | domain — *designed, not built (M0–M7)* |

Reject-only codes (`NO_DEVICE_IDENTITY`, `TIMESTAMP_FUTURE`, `TENANT_MISMATCH`) appear in `errors[]`, not `flags[]`.

## Flag object

```json
{
  "code": "SENSOR_FAULT",
  "severity": "warn",
  "field": "ProbeTemperature.Celsius",
  "value": 327.67,
  "action": "nullified",
  "rule_id": "PROBE-SENTINEL"
}
```

For `fix` actions, `value` is the original and `new_value` the replacement. `action` is one of `nullified`, `fixed`, `location_skipped`, `flagged`, `kept`.

## Duplicate semantics

Key `(tenant_id, device_imei, ts)`. Hash = SHA-256 of canonical JSON of the raw payload (keys sorted, no insignificant whitespace).

- Same key, same hash → 200 `duplicate`, nothing written.
- Same key, different hash → first kept; second stored in `reading_conflicts`; 200 `conflict` + `DUPLICATE_CONFLICT`.
