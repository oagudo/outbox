package outbox

import (
	"math"
	"testing"
	"time"
)

func TestFixedDelay(t *testing.T) {
	tests := []struct {
		name     string
		delay    time.Duration
		attempts []int
	}{
		{
			name:     "standard delay",
			delay:    5 * time.Second,
			attempts: []int{0, 1, 2, 5, 10, 100},
		},
		{
			name:     "zero delay",
			delay:    0,
			attempts: []int{0, 1, 2, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delayFunc := Fixed(tt.delay)

			for _, attempt := range tt.attempts {
				result := delayFunc(attempt)
				if result != tt.delay {
					t.Errorf("FixedDelay(%v) for attempt %d = %v, want %v",
						tt.delay, attempt, result, tt.delay)
				}
			}
		})
	}
}

func TestExponentialDelay(t *testing.T) {
	tests := []struct {
		name     string
		delay    time.Duration
		maxDelay time.Duration
		cases    []struct {
			attempt  int
			expected time.Duration
		}
	}{
		{
			name:     "standard exponential backoff",
			delay:    1 * time.Second,
			maxDelay: 60 * time.Second,
			cases: []struct {
				attempt  int
				expected time.Duration
			}{
				{attempt: 0, expected: 1 * time.Second},
				{attempt: 1, expected: 2 * time.Second},
				{attempt: 2, expected: 4 * time.Second},
				{attempt: 3, expected: 8 * time.Second},
				{attempt: 4, expected: 16 * time.Second},
				{attempt: 5, expected: 32 * time.Second},
				{attempt: 6, expected: 60 * time.Second},
				{attempt: 10, expected: 60 * time.Second},
			},
		},
		{
			name:     "zero max delay",
			delay:    1 * time.Second,
			maxDelay: 0,
			cases: []struct {
				attempt  int
				expected time.Duration
			}{
				{attempt: 0, expected: 0},
				{attempt: 1, expected: 0},
				{attempt: 2, expected: 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delayFunc := Exponential(tt.delay, tt.maxDelay)

			for _, tc := range tt.cases {
				result := delayFunc(tc.attempt)
				if result != tc.expected {
					t.Errorf("ExponentialDelay(%v, %v) for attempt %d = %v, want %v",
						tt.delay, tt.maxDelay, tc.attempt, result, tc.expected)
				}
			}
		})
	}
}

func TestExponentialDelayOverflow(t *testing.T) {
	tests := []struct {
		name     string
		delay    time.Duration
		maxDelay time.Duration
		attempt  int
		expected time.Duration
	}{
		{
			name:     "large initial delay with high attempts",
			delay:    1 << 50,
			maxDelay: math.MaxInt64,
			attempt:  20,      // This would cause overflow: (1<<50) * (2^20) = 1<<70 which overflows int64
			expected: 1 << 62, // only 12 shifts performed
		},
		{
			name:     "maximum possible delay",
			delay:    math.MaxInt64,
			maxDelay: math.MaxInt64,
			attempt:  1, // attempt 1 would overflow
			expected: math.MaxInt64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delayFunc := Exponential(tt.delay, tt.maxDelay)

			result := delayFunc(tt.attempt)

			if result != tt.expected {
				t.Errorf("%s: result %v does not match expected %v", tt.name, result, tt.expected)
			}
		})
	}
}
