package bullmq

import (
	"context"
	"crypto/md5"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
)

// SchedulerOptions configures the JobScheduler.
type SchedulerOptions struct {
	Prefix string
}

// RepeatableJob holds metadata for a scheduled repeatable job.
type RepeatableJob struct {
	Key     string        `json:"key"`
	Name    string        `json:"name"`
	Cron    string        `json:"cron,omitempty"`
	Every   time.Duration `json:"every,omitempty"`
	Next    time.Time     `json:"next"`
	EndDate *time.Time    `json:"endDate,omitempty"`
	Limit   int           `json:"limit"`
	Count   int           `json:"count"`
	EntryID cron.EntryID  `json:"-"`
}

// JobScheduler manages cron and interval-based repeatable jobs.
type JobScheduler struct {
	queue *Queue
	opts  SchedulerOptions
	cron  *cron.Cron
	mu    sync.Mutex
	jobs  map[string]*RepeatableJob
}

// NewJobScheduler creates a new JobScheduler.
func NewJobScheduler(queue *Queue, opts SchedulerOptions) *JobScheduler {
	if opts.Prefix == "" {
		opts.Prefix = queue.prefix
	}
	return &JobScheduler{
		queue: queue,
		opts:  opts,
		cron:  cron.New(cron.WithSeconds()),
		jobs:  make(map[string]*RepeatableJob),
	}
}

// Upsert registers or replaces a repeatable job.
func (s *JobScheduler) Upsert(ctx context.Context, name string, data interface{}, jobOpts JobOptions) (*RepeatableJob, error) {
	repeat := jobOpts.Repeat
	if repeat == nil {
		return nil, fmt.Errorf("bullmq: JobOptions.Repeat required")
	}
	if repeat.Cron == "" && repeat.Every == 0 {
		return nil, fmt.Errorf("bullmq: repeat.Cron or repeat.Every required")
	}

	key := s.repeatKey(name, jobOpts)
	if repeat.Key != "" {
		key = repeat.Key
	}

	s.mu.Lock()
	if existing, ok := s.jobs[key]; ok {
		s.cron.Remove(existing.EntryID)
		delete(s.jobs, key)
	}
	s.mu.Unlock()

	rj := &RepeatableJob{
		Key:     key,
		Name:    name,
		Cron:    repeat.Cron,
		Every:   repeat.Every,
		Limit:   repeat.Limit,
		EndDate: repeat.EndDate,
	}

	addFn := func() {
		s.mu.Lock()
		current, ok := s.jobs[key]
		if !ok {
			s.mu.Unlock()
			return
		}
		if current.EndDate != nil && time.Now().After(*current.EndDate) {
			s.cron.Remove(current.EntryID)
			delete(s.jobs, key)
			s.mu.Unlock()
			return
		}
		if current.Limit > 0 && current.Count >= current.Limit {
			s.cron.Remove(current.EntryID)
			delete(s.jobs, key)
			s.mu.Unlock()
			return
		}
		current.Count++
		s.mu.Unlock()

		addOpts := jobOpts
		addOpts.Repeat = nil
		addOpts.JobID = fmt.Sprintf("%s:%d", key, time.Now().UnixMilli())

		if _, err := s.queue.Add(ctx, name, data, addOpts); err != nil {
			s.queue.EventEmitter.emit(&Event{
				Type: EventError,
				Data: fmt.Errorf("scheduler %q: %w", name, err),
			})
		}
	}

	var entryID cron.EntryID
	var err error

	if repeat.Cron != "" {
		entryID, err = s.cron.AddFunc(repeat.Cron, addFn)
	} else {
		spec := fmt.Sprintf("@every %s", repeat.Every.String())
		entryID, err = s.cron.AddFunc(spec, addFn)
	}
	if err != nil {
		return nil, fmt.Errorf("bullmq: register schedule %q: %w", name, err)
	}

	rj.EntryID = entryID
	rj.Next = s.cron.Entry(entryID).Next

	s.mu.Lock()
	s.jobs[key] = rj
	s.mu.Unlock()

	_ = s.queue.rdb.ZAdd(ctx, s.repeatableKey(), redis.Z{
		Score:  float64(rj.Next.UnixMilli()),
		Member: key,
	}).Err()

	return rj, nil
}

// Remove stops and deletes a repeatable job by key.
func (s *JobScheduler) Remove(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rj, ok := s.jobs[key]
	if !ok {
		return fmt.Errorf("bullmq: repeatable job %q not found", key)
	}
	s.cron.Remove(rj.EntryID)
	delete(s.jobs, key)
	return s.queue.rdb.ZRem(ctx, s.repeatableKey(), key).Err()
}

// GetRepeatableJobs lists all active repeatable jobs.
func (s *JobScheduler) GetRepeatableJobs(_ context.Context) []*RepeatableJob {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*RepeatableJob, 0, len(s.jobs))
	for _, j := range s.jobs {
		cp := *j
		out = append(out, &cp)
	}
	return out
}

// Start begins cron scheduling.
func (s *JobScheduler) Start() { s.cron.Start() }

// Stop halts cron scheduling and waits for running jobs to finish.
func (s *JobScheduler) Stop() context.Context { return s.cron.Stop() }

func (s *JobScheduler) repeatableKey() string {
	return fmt.Sprintf("%s:%s:repeat", s.queue.prefix, s.queue.name)
}

func (s *JobScheduler) repeatKey(name string, opts JobOptions) string {
	r := opts.Repeat
	input := name
	if r.Cron != "" {
		input += ":" + r.Cron
	}
	if r.Every > 0 {
		input += fmt.Sprintf(":every%d", r.Every.Milliseconds())
	}
	h := md5.Sum([]byte(input))
	return fmt.Sprintf("%x", h[:4])
}
