package mcp

// ErrorCode enumerates structured error codes for MCP responses.
type ErrorCode string

const (
	ErrorCodeInvalidTicker     ErrorCode = "INVALID_TICKER"
	ErrorCodeInvalidArgument   ErrorCode = "INVALID_ARGUMENT"
	ErrorCodeNotFound          ErrorCode = "NOT_FOUND"
	ErrorCodeSourceStale       ErrorCode = "SOURCE_STALE"
	ErrorCodeUpstream429       ErrorCode = "UPSTREAM_429"
	ErrorCodeUpstreamQuota     ErrorCode = "UPSTREAM_QUOTA"
	ErrorCodeUpstreamDaily     ErrorCode = "UPSTREAM_DAILY_LIMIT"
	ErrorCodeUpstreamMalformed ErrorCode = "UPSTREAM_MALFORMED"
	ErrorCodeDBUnavailable     ErrorCode = "DB_UNAVAILABLE"
	ErrorCodeUnauthorized      ErrorCode = "UNAUTHORIZED"
	ErrorCodeInternal          ErrorCode = "INTERNAL"
)

// ErrorEnvelope is returned on errors only. Successful calls return data directly.
type ErrorEnvelope struct {
	Error ErrorDetail `json:"error"`
}

type ErrorDetail struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
}

// NewError creates an error envelope.
func NewError(code ErrorCode, message string, retryable bool) ErrorEnvelope {
	return ErrorEnvelope{
		Error: ErrorDetail{
			Code:      code,
			Message:   message,
			Retryable: retryable,
		},
	}
}
