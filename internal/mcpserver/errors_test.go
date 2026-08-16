package mcpserver

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/ipot"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/usecase"
	"github.com/nicholas-audric/idx-mcp-pipeline/pkg/mcp"
)

func TestExceptionToEnvelope(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantCode  mcp.ErrorCode
		wantRetry bool
	}{
		{"invalid ticker", usecase.ErrInvalidTicker, mcp.ErrorCodeInvalidTicker, false},
		{"invalid argument", usecase.ErrInvalidArgument, mcp.ErrorCodeInvalidArgument, false},
		{"invalid range", usecase.ErrInvalidRange, mcp.ErrorCodeInvalidArgument, false},
		{"no trading day", usecase.ErrNoTradingDay, mcp.ErrorCodeNotFound, false},
		{"upstream 429", ipot.ErrUpstream429, mcp.ErrorCodeUpstream429, true},
		{"persist failure", usecase.ErrPersist, mcp.ErrorCodeDBUnavailable, true},
		{"wrapped sentinel", fmt.Errorf("ipot fetch: %w", ipot.ErrUpstream429), mcp.ErrorCodeUpstream429, true},
		{"pg error", &pgconn.PgError{Code: "57P01"}, mcp.ErrorCodeDBUnavailable, true},
		{"conn done", sql.ErrConnDone, mcp.ErrorCodeDBUnavailable, true},
		{"generic", errors.New("boom"), mcp.ErrorCodeInternal, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := exceptionToEnvelope(tc.err)
			if env.Error.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", env.Error.Code, tc.wantCode)
			}
			if env.Error.Retryable != tc.wantRetry {
				t.Fatalf("retryable = %v, want %v", env.Error.Retryable, tc.wantRetry)
			}
			if env.Error.Message == "" {
				t.Fatal("message must not be empty")
			}
		})
	}
}
