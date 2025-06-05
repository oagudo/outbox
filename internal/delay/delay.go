package delay

import (
	"math"
	"time"
)

// DelayFunc is a function that returns the delay after a given attempt.
type DelayFunc func(attempt int) time.Duration

func FixedDelay(delay time.Duration) DelayFunc {
	return func(attempt int) time.Duration {
		return delay
	}
}

func ExponentialDelay(delay time.Duration, maxDelay time.Duration) DelayFunc {
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
