package indexalpha

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nicholas-audric/idx-mcp-pipeline/internal/broker"
)

// Pool turns N Index Alpha keys into one source. It owns the per-key quota
// state and the rotation policy; the Client only speaks HTTP.
//
// State is in memory and seeded from GET /usage (free) at every process boot.
// The vendor's X-Monthly-Remaining header is authoritative after that **when it
// is present**: a successful call refills the counter from it, and an
// exhaustion 403 parks the key until the rolling period ends. The headers were
// observed live on 2026-10-31 (x-monthly-reset is Unix epoch seconds); absence
// is still tolerated, falling back to deducting the call's cost and keeping the
// /usage period_end as the reset. No monthly tier is hardcoded.
type Pool struct {
	client   *Client
	log      *logrus.Logger
	keys     []*keyState
	next     int
	reserve  int
	cooldown time.Duration
	now      func() time.Time

	mu               sync.Mutex
	quotaStopped     bool
	onQuotaExhausted func(QuotaExhaustedEvent)
}

// keyState is one key's quota state. remainingKnown distinguishes a genuine 0
// from "not measured yet": unknown is optimistic (try the key), zero is not.
type keyState struct {
	key            string
	label          string
	remaining      int
	remainingKnown bool
	resetAt        *time.Time
	exhaustedUntil *time.Time
	cooldownUntil  *time.Time
	disabledReason string
	disabledUntil  *time.Time
	plan           string
	needsSeed      bool
	lastUsedAt     *time.Time
	lastError      string
}

// KeyStatus is one key's observable state. Label is masked; the key itself is
// never logged or returned.
type KeyStatus struct {
	Label          string     `json:"label"`
	Plan           string     `json:"plan,omitempty"`
	Remaining      int        `json:"remaining"`
	Unknown        bool       `json:"unknown"`
	Usable         bool       `json:"usable"`
	Exhausted      bool       `json:"exhausted"`
	ResetAt        *time.Time `json:"reset_at,omitempty"`
	CooldownUntil  *time.Time `json:"cooldown_until,omitempty"`
	DisabledReason string     `json:"disabled_reason,omitempty"`
	LastUsedAt     *time.Time `json:"last_used_at,omitempty"`
	LastError      string     `json:"last_error,omitempty"`
}

// Status is the pool's observable state: keys configured, keys usable, quota
// remaining. TotalRemaining sums only known counters; UnknownKeys says how many
// keys have not been measured yet, so Available is a lower bound in that case.
type Status struct {
	Configured     int         `json:"configured"`
	Usable         int         `json:"usable"`
	TotalRemaining int         `json:"total_remaining"`
	UnknownKeys    int         `json:"unknown_keys"`
	Reserve        int         `json:"reserve"`
	Available      int         `json:"available"` // known remaining − reserve, floored at 0
	QuotaStopped   bool        `json:"quota_stopped"`
	NextResetAt    *time.Time  `json:"next_reset_at,omitempty"`
	Keys           []KeyStatus `json:"keys"`
}

// QuotaExhaustedEvent fires once on the transition into "every usable key is
// out of quota". It is the seam a pipeline recorder uses to surface a rolling
// period end as its own condition rather than a parse or network failure.
type QuotaExhaustedEvent struct {
	At      time.Time
	ResetAt *time.Time
	Status  Status
}

// NewPool builds the key pool. Keys are deduplicated in configuration order; a
// non-empty legacy APIKey is a one-key pool. A nil logger is replaced with a
// default so construction never panics.
func NewPool(cfg Config, log *logrus.Logger) *Pool {
	if log == nil {
		log = logrus.New()
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = DefaultCooldown
	}
	if cfg.Reserve < 0 {
		cfg.Reserve = 0
	}

	keys := make([]string, 0, len(cfg.Keys)+1)
	seen := make(map[string]bool)
	for _, k := range append(append([]string{}, cfg.Keys...), cfg.APIKey) {
		k = strings.TrimSpace(k)
		if k == "" || seen[k] {
			continue
		}
		seen[k] = true
		keys = append(keys, k)
	}

	states := make([]*keyState, 0, len(keys))
	for i, k := range keys {
		states = append(states, &keyState{
			key:   k,
			label: fmt.Sprintf("#%d %s", i+1, maskKey(k)),
			// A key with no seed is optimistically usable: /usage failure must
			// not disable a working source.
			needsSeed: true,
		})
	}

	return &Pool{
		client:   NewClient(cfg, log),
		log:      log,
		keys:     states,
		reserve:  cfg.Reserve,
		cooldown: cfg.Cooldown,
		now:      time.Now,
	}
}

// Configured returns the number of keys the pool was built with.
func (p *Pool) Configured() int { return len(p.keys) }

// SetOnQuotaExhausted installs the observer fired on the transition into total
// quota exhaustion. It is safe to call before the pool is used.
func (p *Pool) SetOnQuotaExhausted(fn func(QuotaExhaustedEvent)) {
	p.mu.Lock()
	p.onQuotaExhausted = fn
	p.mu.Unlock()
}

// Seed reads GET /usage once per key. The endpoint is free, so this establishes
// ground truth at boot without spending quota. Failures are logged, collected
// and returned; they never block startup or disable a key.
func (p *Pool) Seed(ctx context.Context) error {
	if len(p.keys) == 0 {
		p.log.Warn("indexalpha pool: no API keys configured (INDEXALPHA_KEYS) — broker calls will report UNAUTHORIZED")
		return nil
	}

	errs := make([]error, len(p.keys))
	var wg sync.WaitGroup
	for i := range p.keys {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = p.seedKey(ctx, i)
		}(i)
	}
	wg.Wait()

	err := errors.Join(errs...)
	st := p.Status()
	reset := "none"
	if st.NextResetAt != nil {
		reset = st.NextResetAt.Format(time.RFC3339)
	}
	p.log.Infof("indexalpha pool: %d keys configured, %d usable, %d units remaining across %d unknown (reserve %d), next reset %s",
		st.Configured, st.Usable, st.TotalRemaining, st.UnknownKeys, st.Reserve, reset)
	return err
}

// seedKey refreshes one key from /usage. needsSeed is cleared before the call so
// a failed seed cannot loop; the key stays optimistically usable.
func (p *Pool) seedKey(ctx context.Context, idx int) error {
	p.mu.Lock()
	ks := p.keys[idx]
	key := ks.key
	label := ks.label
	ks.needsSeed = false
	p.mu.Unlock()

	usage, outcome, err := p.client.Usage(ctx, key)
	now := p.now()

	p.mu.Lock()
	if err != nil {
		err = fmt.Errorf("indexalpha pool: key %s seed: %w", label, err)
		ks.lastError = err.Error()
		p.applyFailureLocked(ks, outcome, err, now)
	} else {
		ks.lastError = ""
		ks.plan = usage.Account.Plan
		ks.remaining = int(usage.Account.Remaining)
		ks.remainingKnown = true
		if usage.Account.PeriodEnd != nil {
			ks.resetAt = usage.Account.PeriodEnd
		}
		active := usage.Account.IsActive == nil || *usage.Account.IsActive
		if !active {
			ks.disabledReason = "subscription inactive"
			ks.disabledUntil = retryAt(ks.resetAt, now)
		} else {
			ks.disabledReason = ""
			ks.disabledUntil = nil
		}
		p.applyRemainingLocked(ks, now)
	}
	ev := p.quotaTransitionLocked(now)
	p.mu.Unlock()

	p.emitQuotaEvent(ev)

	if err != nil {
		p.log.Warnf("%s", err)
		return err
	}
	p.mu.Lock()
	remaining, plan, resetAt := ks.remaining, ks.plan, ks.resetAt
	p.mu.Unlock()
	p.log.Debugf("indexalpha pool: key %s seeded remaining=%d plan=%s reset=%s",
		label, remaining, plan, formatTimePtr(resetAt))
	return nil
}

// Fetch returns the broker summary for one ticker on one trading day, choosing
// and rotating keys through the pool. It satisfies the usecase's
// BrokerSummaryFetcher. A call costs one quota unit.
func (p *Pool) Fetch(ctx context.Context, ticker string, date time.Time) (*broker.Result, error) {
	var res *broker.Result
	err := p.Do(ctx, 1, func(ctx context.Context, key string) (CallOutcome, error) {
		r, outcome, err := p.client.FetchWithKey(ctx, key, ticker, date)
		if err == nil {
			res = r
		}
		return outcome, err
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Do runs one metered call against a usable key, with bounded failover across
// distinct keys within the same call.
//
// Rotation is per successful request: the round-robin pointer advances only
// when fn succeeds. Key-scoped failures mark their key and move on — quota
// parks it until reset, 429 cools it down, 401 disables it. Request-scoped
// failures (400/422, malformed payload, 5xx) are returned immediately: they
// say nothing about the key. The callback receives only the key; it must never
// log it unmasked.
func (p *Pool) Do(ctx context.Context, cost int, fn func(ctx context.Context, key string) (CallOutcome, error)) error {
	if len(p.keys) == 0 {
		return fmt.Errorf("%w: no API keys configured (INDEXALPHA_KEYS)", ErrUnauthorized)
	}
	if cost < 1 {
		cost = 1
	}

	tried := make(map[int]bool, len(p.keys))
	var lastErr error

	for {
		idx, needSeed := p.nextCandidate(tried)
		if idx < 0 {
			return p.noUsableKeyError(lastErr)
		}
		if needSeed {
			// A period rolled over (or the key was never measured): refresh the
			// free usage endpoint, then re-pick. seedKey always clears needsSeed,
			// so this cannot loop on the same key.
			_ = p.seedKey(ctx, idx)
			continue
		}

		tried[idx] = true
		p.mu.Lock()
		key := p.keys[idx].key
		now := p.now()
		p.keys[idx].lastUsedAt = &now
		p.mu.Unlock()

		outcome, err := fn(ctx, key)
		if err == nil {
			p.recordSuccess(idx, outcome, cost)
			return nil
		}

		p.recordFailure(idx, outcome, err)
		lastErr = err
		if !keyScopedFailure(err) {
			return err
		}
	}
}

// nextCandidate returns the next usable key: the least recently used one,
// with the round-robin pointer breaking ties. LRU keeps concurrent callers off
// the same key (sequential calls still alternate, because a successful call
// advances the pointer and stamps last_used_at), while rotation remains tied to
// requests rather than to process start. The bool asks the caller to refresh
// the key from /usage first.
func (p *Pool) nextCandidate(tried map[int]bool) (int, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := p.now()
	p.refreshExpiredLocked(now)

	n := len(p.keys)
	best := -1
	var bestUsed *time.Time
	for off := 0; off < n; off++ {
		i := (p.next + off) % n
		if tried[i] {
			continue
		}
		ks := p.keys[i]
		if usable, _ := p.usableLocked(ks, now); !usable {
			continue
		}
		if best == -1 {
			best, bestUsed = i, ks.lastUsedAt
			continue
		}
		if olderTimestamp(ks.lastUsedAt, bestUsed) {
			best, bestUsed = i, ks.lastUsedAt
		}
	}
	if best == -1 {
		return -1, false
	}
	return best, p.keys[best].needsSeed
}

// olderTimestamp treats nil (never used) as older than any recorded time. Equal
// timestamps keep the round-robin order of the caller's iteration.
func olderTimestamp(a, b *time.Time) bool {
	if a == nil {
		return b != nil
	}
	if b == nil {
		return false
	}
	return a.Before(*b)
}

// usableLocked reports whether a key may be used now, and separately whether its
// unavailability is quota exhaustion (the distinction drives the error class).
func (p *Pool) usableLocked(ks *keyState, now time.Time) (usable bool, exhausted bool) {
	if ks.disabledReason != "" && (ks.disabledUntil == nil || now.Before(*ks.disabledUntil)) {
		return false, false
	}
	if ks.exhaustedUntil != nil && now.Before(*ks.exhaustedUntil) {
		return false, true
	}
	if ks.cooldownUntil != nil && now.Before(*ks.cooldownUntil) {
		return false, false
	}
	if ks.remainingKnown && ks.remaining <= 0 {
		return false, true
	}
	return true, false
}

// refreshExpiredLocked clears time-bounded parkings. A key whose rolling period
// ended becomes usable again and is flagged for a /usage re-seed, so the new
// window's counter is read rather than assumed.
func (p *Pool) refreshExpiredLocked(now time.Time) {
	for _, ks := range p.keys {
		if ks.exhaustedUntil != nil && !now.Before(*ks.exhaustedUntil) {
			ks.exhaustedUntil = nil
			ks.remainingKnown = false
			ks.needsSeed = true
		}
		if ks.disabledUntil != nil && !now.Before(*ks.disabledUntil) {
			ks.disabledReason = ""
			ks.disabledUntil = nil
			ks.needsSeed = true
		}
		if ks.cooldownUntil != nil && !now.Before(*ks.cooldownUntil) {
			ks.cooldownUntil = nil
		}
	}
}

func (p *Pool) recordSuccess(idx int, outcome CallOutcome, cost int) {
	now := p.now()
	p.mu.Lock()
	ks := p.keys[idx]
	ks.lastUsedAt = &now
	ks.lastError = ""

	if outcome.MonthlyRemaining != nil {
		ks.remaining = *outcome.MonthlyRemaining
		ks.remainingKnown = true
	} else if ks.remainingKnown {
		ks.remaining -= cost
		if ks.remaining < 0 {
			ks.remaining = 0
		}
	}
	if outcome.MonthlyReset != nil {
		ks.resetAt = outcome.MonthlyReset
	}
	p.applyRemainingLocked(ks, now)

	// Success is the only event that advances rotation.
	p.next = (idx + 1) % len(p.keys)
	remaining := remainingText(ks)
	ev := p.quotaTransitionLocked(now)
	p.mu.Unlock()

	p.emitQuotaEvent(ev)
	p.log.Debugf("indexalpha pool: key %s succeeded, remaining=%s", ks.label, remaining)
}

func (p *Pool) recordFailure(idx int, outcome CallOutcome, err error) {
	now := p.now()
	p.mu.Lock()
	ks := p.keys[idx]
	ks.lastUsedAt = &now
	p.applyFailureLocked(ks, outcome, err, now)
	ev := p.quotaTransitionLocked(now)
	p.mu.Unlock()

	p.emitQuotaEvent(ev)
	p.log.Debugf("indexalpha pool: key %s failed: %v", ks.label, err)
}

// applyFailureLocked updates key state for a key-scoped failure. Request-scoped
// errors deliberately leave the state untouched.
func (p *Pool) applyFailureLocked(ks *keyState, outcome CallOutcome, err error, now time.Time) {
	ks.lastError = err.Error()

	switch {
	case isQuota(err):
		ks.remaining = 0
		ks.remainingKnown = true
		if outcome.MonthlyReset != nil {
			ks.resetAt = outcome.MonthlyReset
		}
		ks.exhaustedUntil = retryAt(ks.resetAt, now)
	case isRateLimited(err):
		d := p.cooldown
		if outcome.RetryAfter != nil {
			d = *outcome.RetryAfter
		}
		if d <= 0 {
			d = p.cooldown
		}
		until := now.Add(d)
		ks.cooldownUntil = &until
	case isUnauthorized(err):
		ks.disabledReason = "unauthorized"
		ks.disabledUntil = nil
	}
}

// applyRemainingLocked parks a key whose authoritative counter reached zero.
// The period end is the reset; if the vendor did not give one, park for a short
// re-check delay rather than assuming a calendar month.
func (p *Pool) applyRemainingLocked(ks *keyState, now time.Time) {
	if ks.remainingKnown && ks.remaining <= 0 {
		ks.exhaustedUntil = retryAt(ks.resetAt, now)
		return
	}
	ks.exhaustedUntil = nil
}

// quotaTransitionLocked reports the transition into total quota exhaustion,
// exactly once. Any usable key (including an unmeasured one) clears the state.
func (p *Pool) quotaTransitionLocked(now time.Time) *QuotaExhaustedEvent {
	usableAny := false
	quotaAny := false
	for _, ks := range p.keys {
		usable, exhausted := p.usableLocked(ks, now)
		if usable {
			usableAny = true
		}
		if exhausted {
			quotaAny = true
		}
	}

	if usableAny || !quotaAny {
		p.quotaStopped = false
		return nil
	}
	if p.quotaStopped {
		return nil
	}
	p.quotaStopped = true

	st := p.statusLocked(now)
	var reset *time.Time
	for _, ks := range p.keys {
		if ks.exhaustedUntil != nil && (reset == nil || ks.exhaustedUntil.Before(*reset)) {
			reset = ks.exhaustedUntil
		}
	}
	return &QuotaExhaustedEvent{At: now, ResetAt: reset, Status: st}
}

func (p *Pool) emitQuotaEvent(ev *QuotaExhaustedEvent) {
	if ev == nil {
		return
	}
	p.mu.Lock()
	fn := p.onQuotaExhausted
	p.mu.Unlock()
	if fn != nil {
		fn(*ev)
	}
	p.log.Warnf("indexalpha pool: all %d keys are out of quota until %s",
		ev.Status.Configured, formatTimePtr(ev.ResetAt))
}

// noUsableKeyError classifies an exhausted call by why no key could be used.
func (p *Pool) noUsableKeyError(lastErr error) error {
	now := p.now()
	p.mu.Lock()
	p.refreshExpiredLocked(now)
	var usable, quota, rate, auth, other int
	for _, ks := range p.keys {
		u, exhausted := p.usableLocked(ks, now)
		switch {
		case u:
			usable++
		case exhausted:
			quota++
		case ks.cooldownUntil != nil && now.Before(*ks.cooldownUntil):
			rate++
		case ks.disabledReason != "":
			auth++
		default:
			other++
		}
	}
	total := len(p.keys)
	p.mu.Unlock()

	if usable > 0 {
		// A key became usable between the pick and this classification (a
		// concurrent call). Report the last error rather than a false verdict.
		if lastErr != nil {
			return lastErr
		}
		return fmt.Errorf("%w: no usable Index Alpha key", ErrUnauthorized)
	}
	if rate > 0 {
		return fmt.Errorf("%w: all %d Index Alpha keys are rate limited (quota=%d auth=%d other=%d)",
			broker.ErrRateLimited, total, quota, auth, other)
	}
	if quota > 0 {
		return fmt.Errorf("%w: all %d Index Alpha keys are out of quota (rate=%d auth=%d)",
			broker.ErrQuotaExhausted, total, rate, auth)
	}
	if auth+other == total {
		return fmt.Errorf("%w: no usable Index Alpha key (auth=%d other=%d)", ErrUnauthorized, auth, other)
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("%w: no usable Index Alpha key", ErrUnauthorized)
}

// Status returns a snapshot of the pool: keys configured, usable, quota
// remaining. It never mutates the vendor's counters, only clears expired
// parkings so the view is current.
func (p *Pool) Status() Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	p.refreshExpiredLocked(now)
	return p.statusLocked(now)
}

// Available is the caller-consulted ingestion budget: known remaining minus the
// configured reserve. It is a lower bound when UnknownKeys > 0.
func (p *Pool) Available() int { return p.Status().Available }

func (p *Pool) statusLocked(now time.Time) Status {
	st := Status{
		Configured: len(p.keys),
		Reserve:    p.reserve,
		Keys:       make([]KeyStatus, 0, len(p.keys)),
	}
	usableAny, quotaAny := false, false
	for _, ks := range p.keys {
		usable, exhausted := p.usableLocked(ks, now)
		if usable {
			st.Usable++
			usableAny = true
		}
		if exhausted {
			quotaAny = true
		}

		key := KeyStatus{
			Label:          ks.label,
			Plan:           ks.plan,
			Remaining:      ks.remaining,
			Unknown:        !ks.remainingKnown,
			Usable:         usable,
			Exhausted:      exhausted,
			ResetAt:        ks.resetAt,
			CooldownUntil:  ks.cooldownUntil,
			DisabledReason: ks.disabledReason,
			LastUsedAt:     ks.lastUsedAt,
			LastError:      ks.lastError,
		}
		if !ks.remainingKnown {
			key.Remaining = 0
			st.UnknownKeys++
		} else if ks.remaining > 0 && ks.disabledReason == "" {
			// Budget counts keys that can still spend: a cooling key returns in
			// a minute, a disabled key may never return.
			st.TotalRemaining += ks.remaining
		}
		if ks.resetAt != nil && ks.resetAt.After(now) &&
			(st.NextResetAt == nil || ks.resetAt.Before(*st.NextResetAt)) {
			st.NextResetAt = ks.resetAt
		}
		st.Keys = append(st.Keys, key)
	}
	st.Available = st.TotalRemaining - p.reserve
	if st.Available < 0 {
		st.Available = 0
	}
	st.QuotaStopped = !usableAny && quotaAny
	return st
}

// retryAt returns the period end when it is still in the future, otherwise a
// short re-check moment: the counter may lag the period rollover, and a busy
// loop against a free endpoint is worse than one check an hour later.
func retryAt(reset *time.Time, now time.Time) *time.Time {
	if reset != nil && reset.After(now) {
		return reset
	}
	t := now.Add(minQuotaRetryDelay)
	return &t
}

func maskKey(k string) string {
	if len(k) <= 4 {
		return "****"
	}
	return "..." + k[len(k)-4:]
}

func formatTimePtr(t *time.Time) string {
	if t == nil {
		return "none"
	}
	return t.Format(time.RFC3339)
}

func remainingText(ks *keyState) string {
	if !ks.remainingKnown {
		return "unknown"
	}
	return fmt.Sprintf("%d", ks.remaining)
}
