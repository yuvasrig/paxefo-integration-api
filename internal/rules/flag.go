package rules

// Severity of a quality flag.
const (
	SeverityInfo  = "info"
	SeverityWarn  = "warn"
	SeverityError = "error"
)

// Flag codes (specs/001-integration-api/contracts/quality-flags.md).
const (
	CodeInvalidCoordinates   = "INVALID_COORDINATES"
	CodeSensorFault          = "SENSOR_FAULT"
	CodeOutOfRange           = "OUT_OF_RANGE"
	CodeCoercedType          = "COERCED_TYPE"
	CodeInvalidType          = "INVALID_TYPE"
	CodeTimestampSeconds     = "TIMESTAMP_SECONDS"
	CodeDeviceResolvedByName = "DEVICE_RESOLVED_BY_NAME"
	CodeIdentityMismatch     = "IDENTITY_MISMATCH"
	CodeTemperatureFallback  = "TEMPERATURE_FALLBACK"
	CodeTemperatureConverted = "TEMPERATURE_CONVERTED"
	CodeTempExcursion        = "TEMP_EXCURSION"
	CodeDuplicateConflict    = "DUPLICATE_CONFLICT"
)

// Reject codes (HTTP 422, or 400 for INVALID_JSON).
const (
	CodeInvalidJSON      = "INVALID_JSON"
	CodeNoDeviceIdentity = "NO_DEVICE_IDENTITY"
	CodeNoTimestamp      = "NO_TIMESTAMP"
	CodeTimestampFuture  = "TIMESTAMP_FUTURE"
	CodeNoUsableData     = "NO_USABLE_DATA"
)

// Flag actions.
const (
	ActionNullified       = "nullified"
	ActionFixed           = "fixed"
	ActionLocationSkipped = "location_skipped"
	ActionKept            = "kept"
	ActionReplaced        = "replaced" // conflict policy keep_last: the new reading replaced the stored one
	ActionRejected        = "rejected" // conflict policy reject: nothing was written to the readings
)

// Flag records one quality finding. For fixes Value is the original and NewValue the replacement.
type Flag struct {
	Code     string `json:"code"`
	Severity string `json:"severity"`
	Field    string `json:"field,omitempty"`
	Value    any    `json:"value,omitempty"`
	NewValue any    `json:"new_value,omitempty"`
	Action   string `json:"action,omitempty"`
	RuleID   string `json:"rule_id,omitempty"`
}

// Problem is a rejection reason or error returned to the caller.
type Problem struct {
	Code    string `json:"code"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}
