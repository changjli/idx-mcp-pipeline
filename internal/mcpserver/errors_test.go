package mcpserver

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/indexalpha"
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
		{"broker rate limited", broker.ErrRateLimited, mcp.ErrorCodeUpstream429, true},
		{"quota exhausted", broker.ErrQuotaExhausted, mcp.ErrorCodeUpstreamQuota, false},
		{"wrapped quota", fmt.Errorf("broker summary fetch: %w", broker.ErrQuotaExhausted), mcp.ErrorCodeUpstreamQuota, false},
		{"daily request cap", broker.ErrDailyLimit, mcp.ErrorCodeUpstreamDaily, false},
		{"wrapped daily cap", fmt.Errorf("broker summary fetch: %w", broker.ErrDailyLimit), mcp.ErrorCodeUpstreamDaily, false},
		{"upstream unauthorized", indexalpha.ErrUnauthorized, mcp.ErrorCodeUnauthorized, false},
		{"malformed payload", indexalpha.ErrMalformedPayload, mcp.ErrorCodeUpstreamMalformed, false},
		{"persist failure", usecase.ErrPersist, mcp.ErrorCodeDBUnavailable, true},
		{"wrapped sentinel", fmt.Errorf("broker fetch: %w", broker.ErrRateLimited), mcp.ErrorCodeUpstream429, true},
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
