package client

import (
	"math/rand"
	"time"
)

// jitter returns a duration with ±jitterPct uniform jitter applied.
func jitter(d time.Duration, jitterPct float64) time.Duration {
	if jitterPct <= 0 {
		return d
	}
	delta := time.Duration(float64(d) * jitterPct)
	// Uniform jitter in [-delta, +delta].
	offset := time.Duration(rand.Int63n(2*int64(delta)+1)) - delta
	return d + offset
}

// backoffDelay computes the nth retry delay with exponential backoff + jitter.
// n starts at 0 for the first retry.
func backoffDelay(n int, base, max time.Duration) time.Duration {
	// Exponential: base * 2^n
	exp := base << uint(n) // base * 2^n
	if exp > max {
		exp = max
	}
	// ±20% jitter.
	return jitter(exp, 0.20)
}
