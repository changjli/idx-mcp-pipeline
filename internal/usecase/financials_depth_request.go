package usecase

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/entity"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/financials"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// Response-level Tier-2 depth statuses carried by get_financials. They are
// deliberately fewer than the store's run statuses: the caller of a read tool
// needs to know whether depth is here, on its way, broken, or unavailable —
// not which of the sweep's bookkeeping states produced it.
//
//   - observed: the newest stored statement row already carries depth;
//   - pending: depth is missing and an enrichment job is queued (or the
//     cooldown window is still open) — poll the same call later;
//   - failed: the last enrichment attempt failed; a new one will be made once
//     the cooldown expires;
//   - not_covered: the vendor carries no data for this ticker; it is never
//     retried automatically (the operator sweep with include-not-covered is the
//     manual/quarterly retry path).
//
// The store's `partial` and `yahoo_lagging` states read as pending: depth is
// not available yet and the next run (on-demand after the cooldown, or the
// monthly sweep) may still fill it.
const (
	FinancialDepthObserved   = "observed"
	FinancialDepthPending    = "pending"
	FinancialDepthFailed     = "failed"
	FinancialDepthNotCovered = "not_covered"
)

// DefaultFinancialDepthCooldown is the per-ticker window between on-demand
// depth requests. Yahoo's statement data is monthly, so there is nothing to
// gain from a faster retry; six hours bounds a polling client to a handful of
// enqueues a day while still letting a failure retry the same day.
const DefaultFinancialDepthCooldown = 6 * time.Hour

// FinancialDepthEnqueuer is the on-demand enqueue seam: enqueue one
// idx:financials_depth task for a ticker. Implemented by the asynq wiring in
// cmd/mcp-server (adapter over tasks.EnqueueFinancialDepth). The worker owns
// the Yahoo fetch and the persistence; the read path never blocks on it.
type FinancialDepthEnqueuer interface {
	EnqueueFinancialDepth(ticker string) error
}

// FinancialDepthRequestStore is the read-path view of the depth state: the
// ticker's taxonomy row (for not_covered / failed) and the cooldown clock.
// *repository.FinancialDepthRepository satisfies it.
type FinancialDepthRequestStore interface {
	FindTaxonomy(db *sqlx.DB, ticker string) (*entity.TickerFinancialTaxonomy, error)
	FindDepthRequest(db *sqlx.DB, ticker string) (*entity.FinancialDepthRequest, error)
	UpsertDepthRequest(db *sqlx.DB, ticker string, at time.Time) error
}

// FinancialDepthRequestUseCase is the on-demand Tier-2 trigger (issue 07). It
// answers one question per get_financials call — is depth missing, and may I
// queue another attempt? — and never performs an upstream fetch itself.
type FinancialDepthRequestUseCase struct {
	DB       *sqlx.DB
	Log      *logrus.Logger
	Repo     FinancialDepthRequestStore
	Enqueuer FinancialDepthEnqueuer

	// Now is the clock the cooldown is measured against.
	Now func() time.Time
	// Cooldown is the minimum gap between two on-demand requests for one
	// ticker.
	Cooldown time.Duration
}

// FinancialDepthRequestOption configures the request usecase.
type FinancialDepthRequestOption func(*FinancialDepthRequestUseCase)

// WithFinancialDepthCooldown overrides the per-ticker cooldown.
func WithFinancialDepthCooldown(cooldown time.Duration) FinancialDepthRequestOption {
	return func(uc *FinancialDepthRequestUseCase) {
		if cooldown >= 0 {
			uc.Cooldown = cooldown
		}
	}
}

// WithFinancialDepthRequestClock injects the clock.
func WithFinancialDepthRequestClock(now func() time.Time) FinancialDepthRequestOption {
	return func(uc *FinancialDepthRequestUseCase) {
		if now != nil {
			uc.Now = now
		}
	}
}

// NewFinancialDepthRequestUseCase wires the on-demand depth trigger. A nil
// enqueuer disables the trigger (the status is still reported), which keeps
// the read path usable in CLI and test wiring.
func NewFinancialDepthRequestUseCase(
	db *sqlx.DB,
	log *logrus.Logger,
	repo FinancialDepthRequestStore,
	enqueuer FinancialDepthEnqueuer,
	opts ...FinancialDepthRequestOption,
) *FinancialDepthRequestUseCase {
	uc := &FinancialDepthRequestUseCase{
		DB:       db,
		Log:      log,
		Repo:     repo,
		Enqueuer: enqueuer,
		Now:      time.Now,
		Cooldown: DefaultFinancialDepthCooldown,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(uc)
		}
	}
	return uc
}

// RequireDepth returns the ticker's response-level depth_status, enqueueing at
// most one enrichment job when depth is missing. depthMissing is the read
// path's verdict on the newest statement row (no usable depth value), so the
// trigger follows the data actually served, not a wall-clock guess.
//
// The method is best-effort: a store or enqueue failure is returned alongside
// the most conservative status (pending/failed) and the caller logs it rather
// than failing the read.
func (uc *FinancialDepthRequestUseCase) RequireDepth(ctx context.Context, ticker string, depthMissing bool) (string, error) {
	stored, err := uc.Repo.FindTaxonomy(uc.DB, ticker)
	if err != nil {
		return FinancialDepthPending, fmt.Errorf("read depth taxonomy: %w", err)
	}

	// Depth is already on the newest statement row: this is a store hit and no
	// upstream call (or enqueue) is needed.
	if !depthMissing {
		return FinancialDepthObserved, nil
	}

	// A ticker the vendor does not carry is marked and never retried
	// automatically; the operator sweep is the manual/quarterly retry path.
	if stored != nil && stored.DepthStatus == financials.DepthStatusNotCovered {
		return FinancialDepthNotCovered, nil
	}

	status := financialDepthResponseStatus(stored)
	if uc.Enqueuer == nil {
		return status, nil
	}
	if err := ctx.Err(); err != nil {
		return status, err
	}

	now := uc.Now()
	last, err := uc.Repo.FindDepthRequest(uc.DB, ticker)
	if err != nil {
		return status, fmt.Errorf("read depth request: %w", err)
	}
	if last != nil && now.Sub(last.RequestedAt) < uc.Cooldown {
		// Inside the cooldown: report the state we know and do not enqueue.
		return status, nil
	}

	if err := uc.Enqueuer.EnqueueFinancialDepth(ticker); err != nil {
		if !errors.Is(err, asynq.ErrTaskIDConflict) {
			return status, fmt.Errorf("enqueue financial depth: %w", err)
		}
		// The task is already queued/running for this ticker: the caller gets
		// the same pending answer, and the request is stamped as usual.
	}

	if err := uc.Repo.UpsertDepthRequest(uc.DB, ticker, now); err != nil && uc.Log != nil {
		uc.Log.Warnf("financial depth: record on-demand request %s: %v", ticker, err)
	}
	return FinancialDepthPending, nil
}

// financialDepthResponseStatus maps the stored run status to the response
// vocabulary. An absent classification is pending (the first request will
// classify it), and every state that is not a hard failure reports pending.
func financialDepthResponseStatus(stored *entity.TickerFinancialTaxonomy) string {
	if stored != nil && stored.DepthStatus == financials.DepthStatusFailed {
		return FinancialDepthFailed
	}
	return FinancialDepthPending
}

// compile-time check: the concrete repository satisfies the read-path seam.
var _ FinancialDepthRequestStore = (*repository.FinancialDepthRepository)(nil)
