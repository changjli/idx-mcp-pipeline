package client

import (
	"sync"
	"time"
)

// RateLimiter implements a token-bucket rate limiter.
// Thread-safe; one instance shared across all goroutines.
type RateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	rate     float64 // tokens per second
	capacity float64 // burst size
	last     time.Time
}

// NewRateLimiter creates a rate limiter with given rate (tokens/sec).
// Burst is capped at 1 (no bursting) to enforce strict pacing.
func NewRateLimiter(rate float64) *RateLimiter {
	if rate <= 0 {
		rate = 1.0
	}
	return &RateLimiter{
		tokens:   1.0,
		rate:     rate,
		capacity: 1.0,
		last:     time.Now(),
	}
}

// Wait blocks until a token is available.
func (rl *RateLimiter) Wait() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.last).Seconds()
	rl.last = now

	// Refill tokens based on elapsed time.
	rl.tokens += elapsed * rl.rate
	if rl.tokens > rl.capacity {
		rl.tokens = rl.capacity
	}

	// If no token available, sleep (holding lock — sleep is short, ≤1s at 1 req/s).
	if rl.tokens < 1.0 {
		sleep := time.Duration((1.0 - rl.tokens) / rl.rate * float64(time.Second))
		time.Sleep(sleep)
		rl.tokens = 0
	} else {
		rl.tokens -= 1.0
	}
}
