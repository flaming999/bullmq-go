package bullmq

import (
	"math"
	"time"
)

// ComputeBackoff calculates the backoff delay in milliseconds for a given attempt.
func ComputeBackoff(opts *BackoffOptions, attemptsMade int) int64 {
	if opts == nil {
		return 0
	}

	switch opts.Type {
	case BackoffFixed:
		return opts.Delay

	case BackoffExponential:
		// delay * 2^(attemptsMade-1), capped at ~1 hour
		delay := float64(opts.Delay) * math.Pow(2, float64(attemptsMade-1))
		maxDelay := float64(time.Hour.Milliseconds())
		if delay > maxDelay {
			delay = maxDelay
		}
		return int64(delay)

	default:
		return opts.Delay
	}
}
