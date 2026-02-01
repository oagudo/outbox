package outbox

import (
	"math"
	"time"
)

// DelayFunc is a function that returns the delay after a given attempt.
type DelayFunc func(attempt int) time.Duration

// Fixed returns a DelayFunc that returns a fixed delay for all attempts.
func Fixed(delay time.Duration) DelayFunc {
	return func(attempt int) time.Duration {
		return delay
	}
}

// Exponential returns a DelayFunc that returns an exponential delay for all attempts.
// The delay is 2^n where n is the current attempt number.
//
// For example, with initialDelay of 200 milliseconds and maxDelay of 1 hour:
//
// Delay after attempt 0: 200ms
// Delay after attempt 1: 400ms
// Delay after attempt 2: 800ms
// Delay after attempt 3: 1.6s
// Delay after attempt 4: 3.2s
// Delay after attempt 5: 6.4s
// Delay after attempt 6: 12.8s
// Delay after attempt 7: 25.6s
// Delay after attempt 8: 51.2s
// Delay after attempt 9: 1m42.4s
// Delay after attempt 10: 3m24.8s
// Delay after attempt 11: 6m49.6s
// Delay after attempt 12: 13m39.2s
// Delay after attempt 13: 27m18.4s
// Delay after attempt 14: 54m36.8s
// Delay after attempt 15: 1h0m0s
// Delay after attempt 16: 1h0m0s
// ...
func Exponential(delay time.Duration, maxDelay time.Duration) DelayFunc {
	// Pre-calculate max shifts to prevent overflow
	logDelay := math.Floor(math.Log2(float64(delay)))
	var maxShifts uint
	if logDelay >= 62 {
		// If delay is already near maximum, no shifts allowed to prevent overflow
		maxShifts = 0
	} else {
		maxShifts = 62 - uint(logDelay)
	}

	return func(attempt int) time.Duration {
		if attempt == 0 {
			return min(delay, maxDelay)
		}

		// nolint:gosec
		n := min(uint(attempt), maxShifts)

		nextDelay := delay << n
		return min(nextDelay, maxDelay)
	}
}
