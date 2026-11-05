# Tive → PAXAFE mapping

Source of truth: DESIGN.md §6. Stored values keep full precision; rounding happens only when the PAXAFE JSON is emitted. Rounding is half away from zero (`math.Round(x·10ⁿ)/10ⁿ`): -0.5625 → -0.563, 0.6875 → 0.688, 10.078125 → 10.08.

Mapping runs after `rules.Apply` (coercion, sentinels, range checks), so inputs below are already cleaned. A field nulled by a rule maps to `null`.

## Shared fields (sensor and location)

| PAXAFE field | Tive source | Rule |
|---|---|---|
| `device_id` | `DeviceName` | As sent |
| `device_imei` | `DeviceId` | Or the IMEI resolved from `DeviceName` (flag `DEVICE_RESOLVED_BY_NAME`) |
| `timestamp` | `EntryTimeEpoch` | Epoch ms; seconds scaled ×1000 first (flag `TIMESTAMP_SECONDS`) |
| `provider` | — | Constant `"Tive"` |
| `type` | — | Constant `"Active"` |

## Sensor record

| PAXAFE field | Tive source | Rule |
|---|---|---|
| `temperature` | `Temperature.Celsius`, else `ProbeTemperature.Celsius` | 2 dp. Sentinels already nulled. If the device value is null and the probe is usable: use probe, `temperature_source=probe`, flag `TEMPERATURE_FALLBACK`. If `Celsius` is null but `Fahrenheit` present: `(F−32)×5/9` (flag `TEMPERATURE_CONVERTED`). Both null → `null` |
| `humidity` | `Humidity.Percentage` | 1 dp |
| `light_level` | `Light.Lux` | 1 dp |
| `accelerometer.x/y/z` | `Accelerometer.X/Y/Z` | 3 dp |
| `accelerometer.magnitude` | `Accelerometer.G` | 3 dp; if `G` missing and x,y,z all present: √(x²+y²+z²) |
| `accelerometer` | — | Whole object `null` when `Accelerometer` is null/absent |
| `tilt` | — | Always `null` (not provided by Tive) |
| `box_open` | — | Always `null` |

A sensor record is produced when `temperature` is non-null or any of humidity / light / accelerometer is non-null. If every sensor value is null, no sensor record is emitted.

## Location record

| PAXAFE field | Tive source | Rule |
|---|---|---|
| `latitude`, `longitude` | `Location.Latitude/Longitude` | Full precision as sent. Out of range → rule drops the whole location record (`INVALID_COORDINATES`) |
| `altitude` | — | Always `null` |
| `location_accuracy` | `Location.Accuracy.Meters` | Integer (round half away from zero); `null` if absent |
| `location_accuracy_category` | derived from accuracy meters | `High` < 50, `Medium` < 500, else `Low`. Bounds exclusive (500 m → `Low`). Thresholds are config. `null` when accuracy is null |
| `location_source` | `Location.LocationMethod` | `gps`→`GPS`, `wifi`→`WiFi`, `cell`→`Cellular`; `null`/other → `null` |
| `address.full_address` | `Location.FormattedAddress` | Always kept verbatim. `address` is `null` when `FormattedAddress` is null/absent |
| `address.street` | — | Always `null` (matches the PAXAFE schema example) |
| `address.locality/state/postal_code/country` | parsed from `FormattedAddress` | See "Address parsing" |
| `battery_level` | `Battery.Percentage` | Integer 0–100 |
| `cellular_dbm` | `Cellular.Dbm` | 2 dp |
| `cellular_network_type` | — | Always `null` |
| `cellular_operator` | — | Always `null` |
| `wifi_access_points` | `Location.WifiAccessPointUsedCount` | Integer |

## Address parsing (conservative)

Split `FormattedAddress` on `", "`. Only the tail is interpreted; anything unmatched stays `null`.

1. **US form** — last part is a country (`USA`), the part before it matches `^[A-Z]{2} \d{5}(-\d{4})?$`: `state` and `postal_code` from that part, `locality` from the part before it, `country` = last part.
   `"114 Hunts Point Market, Bronx, NY 10474, USA"` → locality `Bronx`, state `NY`, postal `10474`, country `USA`.
2. **City + state/postal, no street** — `"Boston, MA 02101, USA"` → locality `Boston`, state `MA`, postal `02101`, country `USA`.
3. **Two parts** — `"London, UK"` → locality `London`, country `UK`, state/postal `null`.
4. Anything else → only `full_address` is set.

## Kept outside PAXAFE output (stored in tables)

Probe temperature, `Shipment.*` (→ `shipments`), `AccountId` (tenant routing), `Cellular.SignalStrength`, `Battery.Estimation`, `Battery.IsCharging`, `Location.GeolocationSourceName`, `Location.CellTowerUsedCount`, `Accuracy.Kilometers/Miles`, `PublicShipmentId`, `EntityName`.

## Output validation

Every emitted sensor record validates against `assignment/px-sensor-schema.json`, and every location record against `assignment/px-location-schema.json`. A record that fails schema validation is a server bug: event outcome `failed`, HTTP 500, replay after fix.
