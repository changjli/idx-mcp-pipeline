package usecase

import "errors"

// Sentinel errors the MCP server maps to envelope codes via
// exception_to_envelope(). Shared across usecases.
var (
	// ErrInvalidTicker — input failed symbol normalization / pattern check.
	ErrInvalidTicker = errors.New("invalid ticker")
	// ErrNoTradingDay — no stored trading day to resolve a default date from.
	ErrNoTradingDay = errors.New("no trading day found")
	// ErrInvalidRange — a date range with from > to, or an unparseable date.
	ErrInvalidRange = errors.New("invalid date range")
	// ErrInvalidArgument — any other input validation failure (bad date
	// format, out-of-bounds limit). Maps to INVALID_ARGUMENT.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNotFound — the requested row does not exist. Maps to NOT_FOUND.
	ErrNotFound = errors.New("not found")
	// ErrPersist — persisting a fetched broker summary failed (DB write).
	ErrPersist = errors.New("persist broker summary failed")
)
