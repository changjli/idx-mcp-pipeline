package mcpserver

import (
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// newTestValidator returns a TickerValidator with a nil DB, so the universe
// falls back to the bundled ticker list (hermetic — no Postgres needed).
func newTestValidator(t *testing.T) *TickerValidator {
	t.Helper()
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)
	return NewTickerValidator(nil, nil, log)
}

func TestNormalize(t *testing.T) {
	v := newTestValidator(t)

	cases := []struct {
		name  string
		input string
		want  string
		ok    bool
	}{
		{"exact", "BBCA", "BBCA", true},
		{"lowercase", "bbca", "BBCA", true},
		{"jk suffix", "BBCA.JK", "BBCA", true},
		{"lowercase jk suffix", "bbca.jk", "BBCA", true},
		{"whitespace", "  bbca.jk  ", "BBCA", true},
		{"not in universe", "RAJA", "RAJA", false},
		{"empty", "", "", false},
		{"numeric", "123", "123", false},
		{"bare jk", ".JK", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := v.Normalize(tc.input)
			if got != tc.want || ok != tc.ok {
				t.Fatalf("Normalize(%q) = (%q, %v), want (%q, %v)", tc.input, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// TestNormalizeRetriesAfterFailedDBLoad verifies the fallback is not cached
// forever: after a failed/empty DB load the validator re-attempts the DB once
// dbReloadInterval elapses, so a server that starts before the first ticker
// reconcile picks up the real universe without a restart.
func TestNormalizeRetriesAfterFailedDBLoad(t *testing.T) {
	v := newTestValidator(t)

	// First Normalize loads the bundled fallback (nil DB), dbLoaded=false.
	if _, ok := v.Normalize("BBCA"); !ok {
		t.Fatal("BBCA must be valid from the bundled fallback")
	}

	// Inside the retry window the fallback is reused without a reload.
	v.mu.Lock()
	v.lastDBAttempt = time.Now()
	v.mu.Unlock()
	if _, ok := v.Normalize("BBCA"); !ok {
		t.Fatal("fallback must be reused inside the retry window")
	}

	// After the window, load() runs again (still nil DB → bundled reload).
	v.mu.Lock()
	v.lastDBAttempt = time.Now().Add(-dbReloadInterval - time.Second)
	v.mu.Unlock()
	if _, ok := v.Normalize("BBCA"); !ok {
		t.Fatal("universe must reload after the retry window")
	}
}
