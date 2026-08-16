package mcpserver

import (
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/embed"
	"github.com/nicholas-audric/idx-mcp-pipeline/internal/repository"
)

// dbReloadInterval is how long a failed/empty DB universe load keeps the
// bundled fallback before retrying the DB. Bounds DB hammering when the DB is
// down while still picking up the real universe once it's populated.
const dbReloadInterval = 30 * time.Second

// TickerValidator is the symbol normalization seam (ticket 10): one function
// canonicalizes ticker input (.JK suffix, case folding) and validates it
// against the ticker universe before any tool touches a ticker. The universe
// is the DB tickers table (full IDX listing, weekly reconcile); the bundled
// list is the fallback when the DB is empty or unreachable.
//
// A successful non-empty DB load is cached permanently (the universe changes
// only on weekly reconcile). A failed or empty DB load falls back to the
// bundled list but is retried on later calls, so a server that starts before
// the first ticker reconcile — or during a DB outage — picks up the real
// universe without a restart.
type TickerValidator struct {
	db   *sqlx.DB
	repo *repository.TickerRepository
	log  *logrus.Logger

	mu            sync.Mutex
	codes         map[string]struct{}
	dbLoaded      bool      // true once a non-empty DB load succeeded
	lastDBAttempt time.Time // last DB load attempt (for retry pacing)
}

func NewTickerValidator(db *sqlx.DB, repo *repository.TickerRepository, log *logrus.Logger) *TickerValidator {
	return &TickerValidator{db: db, repo: repo, log: log}
}

// Normalize canonicalizes a ticker input and reports whether it is valid.
// Returns the canonical code (uppercase, no .JK suffix) and ok=false for
// invalid input. Invalid tickers never reach a query — the handler returns an
// INVALID_TICKER envelope instead.
func (v *TickerValidator) Normalize(input string) (string, bool) {
	t := strings.ToUpper(strings.TrimSpace(input))
	t = strings.TrimSuffix(t, ".JK")
	if t == "" {
		return "", false
	}
	codes := v.universe()
	_, ok := codes[t]
	return t, ok
}

// universe returns the ticker universe, loading it on first use and re-loading
// after a failed/empty DB attempt once dbReloadInterval has elapsed.
func (v *TickerValidator) universe() map[string]struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.codes != nil && (v.dbLoaded || time.Since(v.lastDBAttempt) < dbReloadInterval) {
		return v.codes
	}
	v.load()
	return v.codes
}

func (v *TickerValidator) load() {
	v.lastDBAttempt = time.Now()
	if v.db != nil && v.repo != nil {
		tickers, err := v.repo.FindAll(v.db)
		if err == nil && len(tickers) > 0 {
			codes := make(map[string]struct{}, len(tickers))
			for _, tk := range tickers {
				codes[tk.Code] = struct{}{}
			}
			v.codes = codes
			v.dbLoaded = true
			return
		}
		if err != nil {
			v.log.Warnf("mcpserver: load ticker universe from DB: %v — using bundled list", err)
		}
	}
	entries, err := embed.LoadTickers()
	if err != nil {
		v.log.Warnf("mcpserver: load bundled ticker list: %v", err)
		v.codes = map[string]struct{}{}
		return
	}
	codes := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		codes[e.Code] = struct{}{}
	}
	v.codes = codes
}
