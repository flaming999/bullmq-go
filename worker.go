package bullmq

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// ProcessFunc is the function signature for job processors.
// Return a value to set as the job's return value, or an error to fail the job.
type ProcessFunc func(ctx context.Context, job *Job) (interface{}, error)

// WorkerOptions configures Worker behavior.
type WorkerOptions struct {
	// Concurrency is the number of jobs processed in parallel. Default: 1
	Concurrency int

	// LockDuration is how long a job lock is held before it must be renewed. Default: 30s
	LockDuration time.Duration

	// LockRenewTime is how often the lock is renewed. Default: LockDuration / 2
	LockRenewTime time.Duration

	// StalledInterval is how often stalled job checks run. Default: 30s
	StalledInterval time.Duration

	// MaxStalledCount is the max times a job can be recovered from stall. Default: 1
	MaxStalledCount int

	// DrainDelay is the polling interval when the queue is empty. Default: 5s
	DrainDelay time.Duration

	// Limiter configures rate limiting.
	Limiter *RateLimiterOptions

	// Autorun starts the worker automatically on creation. Default: true
	AutoRun *bool
}

// RateLimiterOptions configures the worker rate limiter.
type RateLimiterOptions struct {
	// Max is the maximum number of jobs processed per Duration.
	Max int
	// Duration is the time window for the rate limit.
	Duration time.Duration
}

// Worker processes jobs from a queue with configurable concurrency.
type Worker struct {
	queue        *Queue
	processor    ProcessFunc
	opts         WorkerOptions
	EventEmitter *EventEmitter

	ctx    context.Context
	cancel context.CancelFunc

	wg       sync.WaitGroup
	sem      chan struct{}     // concurrency semaphore
	mu       sync.Mutex
	running  bool

	// rate limiter state
	limiterMu    sync.Mutex
	limiterCount int
	limiterReset time.Time
}

// NewWorker creates a new Worker and optionally starts it.
func NewWorker(queue *Queue, processor ProcessFunc, opts WorkerOptions) *Worker {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 1
	}
	if opts.LockDuration == 0 {
		opts.LockDuration = 30 * time.Second
	}
	if opts.LockRenewTime == 0 {
		opts.LockRenewTime = opts.LockDuration / 2
	}
	if opts.StalledInterval == 0 {
		opts.StalledInterval = 30 * time.Second
	}
	if opts.MaxStalledCount == 0 {
		opts.MaxStalledCount = 1
	}
	if opts.DrainDelay == 0 {
		opts.DrainDelay = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	w := &Worker{
		queue:        queue,
		processor:    processor,
		opts:         opts,
		EventEmitter: NewEventEmitter(),
		ctx:          ctx,
		cancel:       cancel,
		sem:          make(chan struct{}, opts.Concurrency),
	}

	if opts.AutoRun == nil || *opts.AutoRun {
		w.Run()
	}

	return w
}

// Run starts the worker's main processing loops.
func (w *Worker) Run() {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}
	w.running = true
	w.mu.Unlock()

	// Main polling loop
	w.wg.Add(1)
	go w.pollLoop()

	// Delayed job promoter
	w.wg.Add(1)
	go w.delayedLoop()

	// Stall checker
	w.wg.Add(1)
	go w.stalledLoop()
}

// Close gracefully shuts down the worker, waiting for in-flight jobs.
func (w *Worker) Close() error {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	w.running = false
	w.mu.Unlock()

	w.cancel()
	w.wg.Wait()
	return nil
}

// Pause temporarily stops picking up new jobs (in-flight jobs finish).
func (w *Worker) Pause(ctx context.Context) error {
	// Signal via queue pause
	return w.queue.Pause(ctx)
}

// Resume allows the worker to pick up jobs again.
func (w *Worker) Resume(ctx context.Context) error {
	return w.queue.Resume(ctx)
}

// pollLoop is the main job-fetching loop.
func (w *Worker) pollLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		// Check if queue is paused
		paused, _ := w.queue.IsPaused(w.ctx)
		if paused {
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(w.opts.DrainDelay):
				continue
			}
		}

		// Rate limiting
		if w.opts.Limiter != nil {
			if !w.checkRateLimit() {
				select {
				case <-w.ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
					continue
				}
			}
		}

		// Acquire concurrency slot
		select {
		case w.sem <- struct{}{}:
		case <-w.ctx.Done():
			return
		}

		// Try to fetch a job
		jobID, err := w.queue.moveToActive(w.ctx)
		if err != nil || jobID == "" {
			// Release slot if no job available
			<-w.sem
			select {
			case <-w.ctx.Done():
				return
			case <-time.After(w.opts.DrainDelay):
				// Emit drained event once
				w.EventEmitter.emit(&Event{Type: EventDrained})
				continue
			}
		}

		// Fetch job data
		job, err := w.queue.GetJob(w.ctx, jobID)
		if err != nil {
			<-w.sem
			w.emitError(fmt.Errorf("bullmq: fetch job %s: %w", jobID, err))
			continue
		}

		// Process job in a goroutine
		go func(j *Job) {
			defer func() { <-w.sem }()
			w.processJob(j)
		}(job)
	}
}

// processJob handles the full lifecycle of a single job.
func (w *Worker) processJob(job *Job) {
	// Set up job context with optional timeout
	jobCtx := w.ctx
	var jobCancel context.CancelFunc

	if job.Opts.Timeout > 0 {
		jobCtx, jobCancel = context.WithTimeout(w.ctx, job.Opts.Timeout)
		defer jobCancel()
	}

	// Lock renewal ticker
	lockDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(w.opts.LockRenewTime)
		defer ticker.Stop()
		for {
			select {
			case <-lockDone:
				return
			case <-ticker.C:
				w.renewLock(job.ID)
			}
		}
	}()

	w.EventEmitter.emit(&Event{Type: EventActive, JobID: job.ID})

	// Catch panics
	var (
		result interface{}
		err    error
	)

	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
			}
		}()
		result, err = w.processor(jobCtx, job)
	}()

	// Stop lock renewal
	close(lockDone)

	if err != nil {
		w.handleFail(job, err)
	} else {
		w.handleComplete(job, result)
	}
}

// handleComplete moves a successfully processed job to completed state.
func (w *Worker) handleComplete(job *Job, result interface{}) {
	if err := w.queue.moveToCompleted(w.ctx, job.ID, result, job.Opts); err != nil {
		w.emitError(fmt.Errorf("bullmq: moveToCompleted %s: %w", job.ID, err))
		return
	}
	job.ReturnValue = result
	w.EventEmitter.emit(&Event{Type: EventCompleted, JobID: job.ID, Data: result})
}

// handleFail moves a failed job to failed state (or retries it).
func (w *Worker) handleFail(job *Job, err error) {
	// Determine if we should retry
	backoffMs := int64(0)
	if job.Opts.Backoff != nil {
		backoffMs = ComputeBackoff(job.Opts.Backoff, job.AttemptsMade)
	}

	reason := err.Error()
	if errors.Is(err, context.DeadlineExceeded) {
		reason = fmt.Sprintf("job timed out after %s", job.Opts.Timeout)
	}

	if moveErr := w.queue.moveToFailed(w.ctx, job, reason, backoffMs); moveErr != nil {
		w.emitError(fmt.Errorf("bullmq: moveToFailed %s: %w", job.ID, moveErr))
		return
	}

	job.FailedReason = reason
	w.EventEmitter.emit(&Event{Type: EventFailed, JobID: job.ID, FailedReason: reason, Data: err})
}

// delayedLoop periodically promotes expired delayed jobs to wait.
func (w *Worker) delayedLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if _, err := w.queue.moveDelayedJobsToWait(w.ctx); err != nil {
				w.emitError(fmt.Errorf("bullmq: moveDelayedToWait: %w", err))
			}
		}
	}
}

// stalledLoop periodically checks for and recovers stalled jobs.
func (w *Worker) stalledLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.opts.StalledInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.queue.checkStalled(w.ctx, w.opts.MaxStalledCount); err != nil {
				w.emitError(fmt.Errorf("bullmq: checkStalled: %w", err))
			}
		}
	}
}

// renewLock extends the lock on an active job to prevent stall detection.
func (w *Worker) renewLock(jobID string) {
	ctx := context.Background()
	stalledKey := w.queue.stalledKey()
	// Re-add to stalled set to reset the clock (in a real impl, would use a separate lock key with TTL)
	_ = w.queue.rdb.SAdd(ctx, stalledKey, jobID).Err()
}

// checkRateLimit returns true if the job can be processed under rate limits.
func (w *Worker) checkRateLimit() bool {
	if w.opts.Limiter == nil {
		return true
	}

	w.limiterMu.Lock()
	defer w.limiterMu.Unlock()

	now := time.Now()
	if now.After(w.limiterReset) {
		w.limiterCount = 0
		w.limiterReset = now.Add(w.opts.Limiter.Duration)
	}

	if w.limiterCount >= w.opts.Limiter.Max {
		return false
	}

	w.limiterCount++
	return true
}

func (w *Worker) emitError(err error) {
	w.EventEmitter.emit(&Event{Type: EventError, Data: err})
}

// IsRunning returns whether the worker is currently running.
func (w *Worker) IsRunning() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.running
}

// Concurrency returns the configured concurrency level.
func (w *Worker) Concurrency() int {
	return w.opts.Concurrency
}
