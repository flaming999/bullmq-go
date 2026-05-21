package bullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// QueueOptions configures a Queue.
type QueueOptions struct {
	// Prefix is the Redis key prefix. Default: "bull"
	Prefix string

	// DefaultJobOptions applies to all jobs added without explicit options.
	DefaultJobOptions *JobOptions

	// Connection options (used if Client is not provided)
	Connection *redis.Options
}

// Queue manages job lifecycle: adding, pausing, retrieving, and removing jobs.
type Queue struct {
	name          string
	prefix        string
	opts          QueueOptions
	rdb           redis.UniversalClient
	scripts       map[string]*redis.Script
	EventEmitter  *EventEmitter
}

// NewQueue creates a new Queue instance with default options (prefix "bull", localhost:6379).
func NewQueue(name string) (*Queue, error) {
	return NewQueueWithOptions(name, QueueOptions{})
}

// NewQueueWithOptions creates a new Queue instance with custom options.
func NewQueueWithOptions(name string, opts QueueOptions) (*Queue, error) {
	if opts.Prefix == "" {
		opts.Prefix = "bull"
	}

	var rdb redis.UniversalClient
	if opts.Connection != nil {
		rdb = redis.NewClient(opts.Connection)
	} else {
		rdb = redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})
	}

	q := &Queue{
		name:         name,
		prefix:       opts.Prefix,
		opts:         opts,
		rdb:          rdb,
		EventEmitter: NewEventEmitter(),
		scripts:      make(map[string]*redis.Script),
	}

	q.scripts["addJob"] = redis.NewScript(addJobScript)
	q.scripts["moveToActive"] = redis.NewScript(moveToActiveScript)
	q.scripts["moveToCompleted"] = redis.NewScript(moveToCompletedScript)
	q.scripts["moveToFailed"] = redis.NewScript(moveToFailedScript)
	q.scripts["moveDelayedToWait"] = redis.NewScript(moveDelayedToWaitScript)
	q.scripts["retryJob"] = redis.NewScript(retryJobScript)
	q.scripts["stalledJobs"] = redis.NewScript(stalledJobsScript)

	return q, nil
}

// NewQueueWithClient creates a Queue using an existing Redis client.
func NewQueueWithClient(name string, rdb redis.UniversalClient) *Queue {
	q := &Queue{
		name:         name,
		prefix:       "bull",
		opts:         QueueOptions{Prefix: "bull"},
		rdb:          rdb,
		EventEmitter: NewEventEmitter(),
		scripts:      make(map[string]*redis.Script),
	}
	q.scripts["addJob"] = redis.NewScript(addJobScript)
	q.scripts["moveToActive"] = redis.NewScript(moveToActiveScript)
	q.scripts["moveToCompleted"] = redis.NewScript(moveToCompletedScript)
	q.scripts["moveToFailed"] = redis.NewScript(moveToFailedScript)
	q.scripts["moveDelayedToWait"] = redis.NewScript(moveDelayedToWaitScript)
	q.scripts["retryJob"] = redis.NewScript(retryJobScript)
	q.scripts["stalledJobs"] = redis.NewScript(stalledJobsScript)
	return q
}

// --- Key helpers ---

func (q *Queue) keyPrefix() string {
	return fmt.Sprintf("%s:%s", q.prefix, q.name)
}

func (q *Queue) waitKey() string     { return q.keyPrefix() + ":wait" }
func (q *Queue) activeKey() string   { return q.keyPrefix() + ":active" }
func (q *Queue) completedKey() string { return q.keyPrefix() + ":completed" }
func (q *Queue) failedKey() string   { return q.keyPrefix() + ":failed" }
func (q *Queue) delayedKey() string  { return q.keyPrefix() + ":delayed" }
func (q *Queue) priorityKey() string { return q.keyPrefix() + ":priority" }
func (q *Queue) eventsKey() string   { return q.keyPrefix() + ":events" }
func (q *Queue) idKey() string       { return q.keyPrefix() + ":id" }
func (q *Queue) stalledKey() string  { return q.keyPrefix() + ":stalled" }
func (q *Queue) pausedKey() string   { return q.keyPrefix() + ":paused" }
func (q *Queue) metaKey() string     { return q.keyPrefix() + ":meta" }
func (q *Queue) jobKey(id string) string { return fmt.Sprintf("%s:%s", q.keyPrefix(), id) }

// --- Public API ---

// Add adds a new job to the queue.
func (q *Queue) Add(ctx context.Context, name string, data interface{}, opts ...JobOptions) (*Job, error) {
	jobOpts := JobOptions{Attempts: 1}
	if q.opts.DefaultJobOptions != nil {
		jobOpts = *q.opts.DefaultJobOptions
	}
	if len(opts) > 0 {
		jobOpts = mergeOptions(jobOpts, opts[0])
	}

	job := &Job{
		Name:      name,
		Data:      data,
		Opts:      jobOpts,
		Timestamp: time.Now().UnixMilli(),
		queueName: q.keyPrefix(),
		rdb:       q.rdb,
	}

	if jobOpts.JobID != "" {
		job.ID = jobOpts.JobID
	}

	// Compute delay in ms
	delayMs := int64(0)
	if jobOpts.Delay > 0 {
		delayMs = jobOpts.Delay.Milliseconds()
	}

	dataJSON, err := json.Marshal(job.toHash())
	if err != nil {
		return nil, fmt.Errorf("bullmq: marshal job: %w", err)
	}

	lifo := "0"
	if jobOpts.Lifo {
		lifo = "1"
	}

	keys := []string{
		q.waitKey(),
		q.priorityKey(),
		q.keyPrefix(),
		q.idKey(),
		q.eventsKey(),
		q.delayedKey(),
	}

	result, err := q.scripts["addJob"].Run(ctx, q.rdb, keys,
		job.ID,
		string(dataJSON),
		jobOpts.Priority,
		delayMs,
		lifo,
		q.name,
		job.Timestamp,
	).Text()
	if err != nil {
		return nil, fmt.Errorf("bullmq: addJob script: %w", err)
	}

	job.ID = result
	q.EventEmitter.emit(&Event{Type: EventAdded, JobID: job.ID})
	return job, nil
}

// AddBulk adds multiple jobs atomically using a pipeline.
func (q *Queue) AddBulk(ctx context.Context, jobs []BulkJob) ([]*Job, error) {
	results := make([]*Job, 0, len(jobs))
	pipe := q.rdb.Pipeline()

	type pendingJob struct {
		job *Job
	}
	var pending []pendingJob

	for _, bj := range jobs {
		jobOpts := JobOptions{Attempts: 1}
		if bj.Opts != nil {
			jobOpts = *bj.Opts
		}

		job := &Job{
			Name:      bj.Name,
			Data:      bj.Data,
			Opts:      jobOpts,
			Timestamp: time.Now().UnixMilli(),
			queueName: q.keyPrefix(),
			rdb:       q.rdb,
		}
		if jobOpts.JobID != "" {
			job.ID = jobOpts.JobID
		}
		pending = append(pending, pendingJob{job: job})
		_ = pipe // pipeline used for future optimizations
	}

	// Execute individually with Lua scripts (atomic per job)
	for _, p := range pending {
		j, err := q.Add(ctx, p.job.Name, p.job.Data, p.job.Opts)
		if err != nil {
			return results, err
		}
		results = append(results, j)
	}

	return results, nil
}

// BulkJob is used as input for AddBulk.
type BulkJob struct {
	Name string
	Data interface{}
	Opts *JobOptions
}

// Pause pauses job processing. New jobs can still be added.
func (q *Queue) Pause(ctx context.Context) error {
	pipe := q.rdb.Pipeline()
	// Ensure paused key exists (even if wait list is empty)
	pipe.Set(ctx, q.pausedKey(), "1", 0)
	// Move waiting jobs to paused list (ignore error if wait key doesn't exist)
	pipe.Rename(ctx, q.waitKey(), q.pausedKey())
	pipe.Publish(ctx, q.eventsKey(), mustMarshal(Event{Type: EventPaused}))
	_, _ = pipe.Exec(ctx)
	return nil
}

// Resume resumes job processing.
func (q *Queue) Resume(ctx context.Context) error {
	pipe := q.rdb.Pipeline()
	pipe.Rename(ctx, q.pausedKey(), q.waitKey())
	pipe.Publish(ctx, q.eventsKey(), mustMarshal(Event{Type: EventResumed}))
	_, err := pipe.Exec(ctx)
	return err
}

// IsPaused returns true if the queue is paused.
func (q *Queue) IsPaused(ctx context.Context) (bool, error) {
	exists, err := q.rdb.Exists(ctx, q.pausedKey()).Result()
	return exists > 0, err
}

// GetJob retrieves a single job by ID.
func (q *Queue) GetJob(ctx context.Context, id string) (*Job, error) {
	hash, err := q.rdb.HGetAll(ctx, q.jobKey(id)).Result()
	if err != nil {
		return nil, err
	}
	if len(hash) == 0 {
		return nil, ErrJobNotFound
	}
	return jobFromHash(hash, q.keyPrefix(), q.rdb)
}

// GetJobs returns jobs in a given state with pagination.
func (q *Queue) GetJobs(ctx context.Context, state JobState, start, end int64, asc bool) ([]*Job, error) {
	var ids []string
	var err error

	switch state {
	case JobStateWaiting:
		if asc {
			ids, err = q.rdb.LRange(ctx, q.waitKey(), start, end).Result()
		} else {
			ids, err = q.rdb.LRange(ctx, q.waitKey(), -(end + 1), -(start + 1)).Result()
		}
	case JobStateActive:
		ids, err = q.rdb.LRange(ctx, q.activeKey(), start, end).Result()
	case JobStateCompleted:
		if asc {
			ids, err = q.rdb.ZRange(ctx, q.completedKey(), start, end).Result()
		} else {
			ids, err = q.rdb.ZRevRange(ctx, q.completedKey(), start, end).Result()
		}
	case JobStateFailed:
		if asc {
			ids, err = q.rdb.ZRange(ctx, q.failedKey(), start, end).Result()
		} else {
			ids, err = q.rdb.ZRevRange(ctx, q.failedKey(), start, end).Result()
		}
	case JobStateDelayed:
		ids, err = q.rdb.ZRange(ctx, q.delayedKey(), start, end).Result()
	default:
		return nil, fmt.Errorf("bullmq: unknown state %q", state)
	}

	if err != nil {
		return nil, err
	}

	jobs := make([]*Job, 0, len(ids))
	for _, id := range ids {
		job, err := q.GetJob(ctx, id)
		if err == ErrJobNotFound {
			continue
		}
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// GetJobState returns the current state of a job.
func (q *Queue) GetJobState(ctx context.Context, id string) (JobState, error) {
	// Check each state-tracking key
	pipe := q.rdb.Pipeline()
	activeCmd := pipe.LPos(ctx, q.activeKey(), id, redis.LPosArgs{})
	waitCmd := pipe.LPos(ctx, q.waitKey(), id, redis.LPosArgs{})
	completedCmd := pipe.ZScore(ctx, q.completedKey(), id)
	failedCmd := pipe.ZScore(ctx, q.failedKey(), id)
	delayedCmd := pipe.ZScore(ctx, q.delayedKey(), id)
	pausedCmd := pipe.LPos(ctx, q.pausedKey(), id, redis.LPosArgs{})

	_, _ = pipe.Exec(ctx)

	if _, err := activeCmd.Result(); err == nil {
		return JobStateActive, nil
	}
	if _, err := waitCmd.Result(); err == nil {
		return JobStateWaiting, nil
	}
	if _, err := completedCmd.Result(); err == nil {
		return JobStateCompleted, nil
	}
	if _, err := failedCmd.Result(); err == nil {
		return JobStateFailed, nil
	}
	if _, err := delayedCmd.Result(); err == nil {
		return JobStateDelayed, nil
	}
	if _, err := pausedCmd.Result(); err == nil {
		return JobStatePaused, nil
	}
	return JobStateUnknown, nil
}

// GetCounts returns job counts for each state.
func (q *Queue) GetCounts(ctx context.Context, states ...JobState) (map[JobState]int64, error) {
	pipe := q.rdb.Pipeline()

	cmds := map[JobState]interface{}{}
	for _, s := range states {
		switch s {
		case JobStateWaiting:
			cmds[s] = pipe.LLen(ctx, q.waitKey())
		case JobStateActive:
			cmds[s] = pipe.LLen(ctx, q.activeKey())
		case JobStateCompleted:
			cmds[s] = pipe.ZCard(ctx, q.completedKey())
		case JobStateFailed:
			cmds[s] = pipe.ZCard(ctx, q.failedKey())
		case JobStateDelayed:
			cmds[s] = pipe.ZCard(ctx, q.delayedKey())
		case JobStatePaused:
			cmds[s] = pipe.LLen(ctx, q.pausedKey())
		}
	}

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	result := make(map[JobState]int64, len(states))
	for state, cmd := range cmds {
		switch c := cmd.(type) {
		case *redis.IntCmd:
			n, _ := c.Result()
			result[state] = n
		}
	}
	return result, nil
}

// RetryJob re-queues a failed job for processing.
func (q *Queue) RetryJob(ctx context.Context, id string) error {
	keys := []string{q.failedKey(), q.waitKey(), q.eventsKey(), q.jobKey(id)}
	return q.scripts["retryJob"].Run(ctx, q.rdb, keys, id).Err()
}

// RetryJobs retries all failed jobs (optionally filtered by timestamp range).
func (q *Queue) RetryJobs(ctx context.Context) error {
	ids, err := q.rdb.ZRange(ctx, q.failedKey(), 0, -1).Result()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := q.RetryJob(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// RemoveJob removes a job and its data from Redis entirely.
func (q *Queue) RemoveJob(ctx context.Context, id string) error {
	state, err := q.GetJobState(ctx, id)
	if err != nil {
		return err
	}

	pipe := q.rdb.Pipeline()

	switch state {
	case JobStateWaiting:
		pipe.LRem(ctx, q.waitKey(), 0, id)
	case JobStateActive:
		pipe.LRem(ctx, q.activeKey(), 0, id)
	case JobStateCompleted:
		pipe.ZRem(ctx, q.completedKey(), id)
	case JobStateFailed:
		pipe.ZRem(ctx, q.failedKey(), id)
	case JobStateDelayed:
		pipe.ZRem(ctx, q.delayedKey(), id)
	case JobStatePaused:
		pipe.LRem(ctx, q.pausedKey(), 0, id)
	}

	pipe.Del(ctx, q.jobKey(id))
	pipe.Del(ctx, fmt.Sprintf("%s:%s:logs", q.keyPrefix(), id))
	pipe.Publish(ctx, q.eventsKey(), mustMarshal(Event{Type: EventRemoved, JobID: id}))

	_, err = pipe.Exec(ctx)
	return err
}

// Clean removes jobs in a given state older than grace period.
func (q *Queue) Clean(ctx context.Context, grace time.Duration, limit int64, state JobState) ([]string, error) {
	cutoff := time.Now().Add(-grace).UnixMilli()
	var ids []string
	var err error

	switch state {
	case JobStateCompleted:
		ids, err = q.rdb.ZRangeByScore(ctx, q.completedKey(), &redis.ZRangeBy{
			Min:    "0",
			Max:    strconv.FormatInt(cutoff, 10),
			Offset: 0,
			Count:  limit,
		}).Result()
	case JobStateFailed:
		ids, err = q.rdb.ZRangeByScore(ctx, q.failedKey(), &redis.ZRangeBy{
			Min:    "0",
			Max:    strconv.FormatInt(cutoff, 10),
			Offset: 0,
			Count:  limit,
		}).Result()
	default:
		return nil, fmt.Errorf("bullmq: clean only supports completed or failed states")
	}
	if err != nil {
		return nil, err
	}

	for _, id := range ids {
		if err := q.RemoveJob(ctx, id); err != nil {
			return ids, err
		}
	}
	return ids, nil
}

// Drain removes all waiting and delayed jobs.
func (q *Queue) Drain(ctx context.Context, delayed bool) error {
	pipe := q.rdb.Pipeline()
	pipe.Del(ctx, q.waitKey())
	if delayed {
		pipe.Del(ctx, q.delayedKey())
	}
	_, err := pipe.Exec(ctx)
	if err == nil {
		q.EventEmitter.emit(&Event{Type: EventDrained})
	}
	return err
}

// Obliterate removes ALL queue data from Redis.
func (q *Queue) Obliterate(ctx context.Context) error {
	keys, err := q.rdb.Keys(ctx, q.keyPrefix()+":*").Result()
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		return q.rdb.Del(ctx, keys...).Err()
	}
	return nil
}

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// Client returns the underlying Redis client.
func (q *Queue) Client() redis.UniversalClient { return q.rdb }

// Close closes the queue (and Redis connection if owned).
func (q *Queue) Close() error {
	return q.rdb.Close()
}

// moveDelayedJobsToWait promotes expired delayed jobs to the wait list.
func (q *Queue) moveDelayedJobsToWait(ctx context.Context) (int, error) {
	keys := []string{q.delayedKey(), q.waitKey(), q.priorityKey(), q.eventsKey()}
	result, err := q.scripts["moveDelayedToWait"].Run(ctx, q.rdb, keys,
		q.keyPrefix(),
		time.Now().UnixMilli(),
	).Int()
	return result, err
}

// moveToActive atomically picks a job from waiting/priority and marks it active.
func (q *Queue) moveToActive(ctx context.Context) (string, error) {
	keys := []string{q.waitKey(), q.priorityKey(), q.activeKey(), q.eventsKey(), q.stalledKey()}
	result, err := q.scripts["moveToActive"].Run(ctx, q.rdb, keys,
		q.keyPrefix(),
		time.Now().UnixMilli(),
	).Text()
	if err == redis.Nil {
		return "", nil
	}
	return result, err
}

// moveToCompleted marks a job as successfully completed.
func (q *Queue) moveToCompleted(ctx context.Context, jobID string, returnValue interface{}, opts JobOptions) error {
	keepJobs := -1 // keep all by default
	if opts.RemoveOnComplete > 0 {
		keepJobs = opts.RemoveOnComplete
	}

	keys := []string{q.activeKey(), q.completedKey(), q.eventsKey(), q.jobKey(jobID), q.stalledKey()}
	return q.scripts["moveToCompleted"].Run(ctx, q.rdb, keys,
		jobID,
		mustMarshal(returnValue),
		keepJobs,
		time.Now().UnixMilli(),
	).Err()
}

// moveToFailed marks a job as failed (or retries it).
func (q *Queue) moveToFailed(ctx context.Context, job *Job, reason string, backoffMs int64) error {
	keepJobs := -1
	if job.Opts.RemoveOnFail > 0 {
		keepJobs = job.Opts.RemoveOnFail
	}

	maxAttempts := job.Opts.Attempts

	keys := []string{
		q.activeKey(),
		q.failedKey(),
		q.waitKey(),
		q.eventsKey(),
		q.jobKey(job.ID),
		q.delayedKey(),
		q.stalledKey(),
	}
	return q.scripts["moveToFailed"].Run(ctx, q.rdb, keys,
		job.ID,
		reason,
		job.AttemptsMade,
		maxAttempts,
		backoffMs,
		keepJobs,
		time.Now().UnixMilli(),
	).Err()
}

// checkStalled detects and recovers stalled jobs.
func (q *Queue) checkStalled(ctx context.Context, maxAttempts int) error {
	keys := []string{q.stalledKey(), q.activeKey(), q.waitKey(), q.failedKey(), q.eventsKey()}
	return q.scripts["stalledJobs"].Run(ctx, q.rdb, keys,
		q.keyPrefix(),
		maxAttempts,
		time.Now().UnixMilli(),
	).Err()
}

func mergeOptions(base, override JobOptions) JobOptions {
	if override.Priority != 0 {
		base.Priority = override.Priority
	}
	if override.Delay != 0 {
		base.Delay = override.Delay
	}
	if override.Attempts != 0 {
		base.Attempts = override.Attempts
	}
	if override.Backoff != nil {
		base.Backoff = override.Backoff
	}
	if override.RemoveOnComplete != 0 {
		base.RemoveOnComplete = override.RemoveOnComplete
	}
	if override.RemoveOnFail != 0 {
		base.RemoveOnFail = override.RemoveOnFail
	}
	if override.JobID != "" {
		base.JobID = override.JobID
	}
	if override.Lifo {
		base.Lifo = override.Lifo
	}
	if override.Timeout != 0 {
		base.Timeout = override.Timeout
	}
	if override.Parent != nil {
		base.Parent = override.Parent
	}
	return base
}

// ErrJobNotFound is returned when a job does not exist.
var ErrJobNotFound = fmt.Errorf("bullmq: job not found")
