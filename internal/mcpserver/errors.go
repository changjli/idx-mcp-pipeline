package mcpserver

import (
	"database/sql"
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/indexalpha"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

// exceptionToEnvelope is the single error-to-envelope seam at the MCP boundary
// (ticket 10): every handler funnels its error through here, so the code
// mapping lives in one place. True errors return the structured envelope;
// successful calls never do.
func exceptionToEnvelope(err error) mcp.ErrorEnvelope {
	switch {
	case errors.Is(err, usecase.ErrInvalidTicker):
		return mcp.NewError(mcp.ErrorCodeInvalidTicker, err.Error(), false)
	case errors.Is(err, usecase.ErrInvalidArgument):
		return mcp.NewError(mcp.ErrorCodeInvalidArgument, err.Error(), false)
	case errors.Is(err, usecase.ErrInvalidRange):
		return mcp.NewError(mcp.ErrorCodeInvalidArgument, err.Error(), false)
	case errors.Is(err, usecase.ErrNoTradingDay):
		return mcp.NewError(mcp.ErrorCodeNotFound, err.Error(), false)
	case errors.Is(err, usecase.ErrNotFound):
		return mcp.NewError(mcp.ErrorCodeNotFound, err.Error(), false)
	case errors.Is(err, broker.ErrQuotaExhausted):
		// Monthly ticker quota is spent until the vendor's reset window;
		// retrying now would just burn another failed request.
		return mcp.NewError(mcp.ErrorCodeUpstreamQuota, err.Error(), false)
	case errors.Is(err, broker.ErrDailyLimit):
		// The per-key daily request cap recovers at local midnight, not at the
		// monthly period end. Distinct code so a caller can pace differently.
		return mcp.NewError(mcp.ErrorCodeUpstreamDaily, err.Error(), false)
	case errors.Is(err, broker.ErrRateLimited):
		return mcp.NewError(mcp.ErrorCodeUpstream429, err.Error(), true)
	case errors.Is(err, indexalpha.ErrUnauthorized):
		return mcp.NewError(mcp.ErrorCodeUnauthorized, err.Error(), false)
	case errors.Is(err, indexalpha.ErrMalformedPayload):
		return mcp.NewError(mcp.ErrorCodeUpstreamMalformed, err.Error(), false)
	case errors.Is(err, usecase.ErrPersist):
		return mcp.NewError(mcp.ErrorCodeDBUnavailable, err.Error(), true)
	case isDBError(err):
		return mcp.NewError(mcp.ErrorCodeDBUnavailable, err.Error(), true)
	default:
		return mcp.NewError(mcp.ErrorCodeInternal, err.Error(), false)
	}
}

// isDBError reports whether the error chain is a Postgres failure (connection
// or query-level), which maps to DB_UNAVAILABLE.
func isDBError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return true
	}
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	return errors.Is(err, sql.ErrConnDone) || errors.Is(err, sql.ErrTxDone)
}
