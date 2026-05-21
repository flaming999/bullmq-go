package bullmq_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/flaming999/bullmq-go"
)

func setupRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	return rdb
}

func TestBasicQueueAndWorker(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-basic", rdb)
	queue.Obliterate(ctx)

	for i := 0; i < 5; i++ {
		job, err := queue.Add(ctx, "test-job", map[string]interface{}{"i": i})
		if err != nil {
			t.Fatalf("Add job: %v", err)
		}
		t.Logf("added job %s", job.ID)
	}

	worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
		return "done", nil
	}, bullmq.WorkerOptions{
		Concurrency: 3,
		DrainDelay:  200 * time.Millisecond,
	})

	worker.EventEmitter.On(bullmq.EventCompleted, func(e *bullmq.Event) {
		t.Logf("completed job %s", e.JobID)
	})

	defer worker.Close()

	time.Sleep(2 * time.Second)

	counts, _ := queue.GetCounts(ctx, bullmq.JobStateCompleted)
	if counts[bullmq.JobStateCompleted] != 5 {
		t.Errorf("expected 5 completed jobs, got %d", counts[bullmq.JobStateCompleted])
	}
}

func TestPriorityJobs(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-priority", rdb)
	queue.Obliterate(ctx)

	_, _ = queue.Add(ctx, "low", nil, bullmq.JobOptions{Priority: 10})
	_, _ = queue.Add(ctx, "high", nil, bullmq.JobOptions{Priority: 1})
	_, _ = queue.Add(ctx, "mid", nil, bullmq.JobOptions{Priority: 5})

	var order []string
	worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
		order = append(order, job.Name)
		return nil, nil
	}, bullmq.WorkerOptions{
		Concurrency: 1,
		DrainDelay:  200 * time.Millisecond,
	})

	defer worker.Close()

	time.Sleep(2 * time.Second)

	if len(order) != 3 {
		t.Fatalf("expected 3 jobs processed, got %d: %v", len(order), order)
	}
	if order[0] != "high" {
		t.Errorf("expected first job 'high', got %q", order[0])
	}
	if order[1] != "mid" {
		t.Errorf("expected second job 'mid', got %q", order[1])
	}
}

func TestDelayedJob(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-delayed", rdb)
	queue.Obliterate(ctx)

	worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
		return nil, nil
	}, bullmq.WorkerOptions{
		Concurrency: 1,
		DrainDelay:  200 * time.Millisecond,
	})
	defer worker.Close()

	before := time.Now()
	_, err := queue.Add(ctx, "delayed", nil, bullmq.JobOptions{Delay: 1 * time.Second})
	if err != nil {
		t.Fatalf("Add delayed job: %v", err)
	}

	time.Sleep(5 * time.Second)

	counts, _ := queue.GetCounts(ctx, bullmq.JobStateCompleted)
	if counts[bullmq.JobStateCompleted] != 1 {
		t.Errorf("expected 1 completed, got %d", counts[bullmq.JobStateCompleted])
	}
	elapsed := time.Since(before)
	if elapsed < 900*time.Millisecond {
		t.Errorf("job processed too early: %v", elapsed)
	}
}

func TestRetryWithBackoff(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-retry", rdb)
	queue.Obliterate(ctx)

	var attempts int
	worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("fail attempt %d", attempts)
		}
		return "ok", nil
	}, bullmq.WorkerOptions{
		Concurrency: 1,
		DrainDelay:  200 * time.Millisecond,
	})
	defer worker.Close()

	_, err := queue.Add(ctx, "flaky", nil, bullmq.JobOptions{
		Attempts: 5,
		Backoff: &bullmq.BackoffOptions{
			Type:  bullmq.BackoffFixed,
			Delay: 500,
		},
	})
	if err != nil {
		t.Fatalf("Add job: %v", err)
	}

	time.Sleep(8 * time.Second)

	if attempts < 3 {
		t.Errorf("expected at least 3 attempts, got %d", attempts)
	}
}

func TestJobProgress(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-progress", rdb)
	queue.Obliterate(ctx)

	_, err := queue.Add(ctx, "progress-job", nil)
	if err != nil {
		t.Fatalf("Add job: %v", err)
	}

	processed := make(chan struct{}, 1)
	worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
		for i := 0; i <= 100; i += 50 {
			_ = job.UpdateProgress(ctx, i)
		}
		processed <- struct{}{}
		return nil, nil
	}, bullmq.WorkerOptions{
		Concurrency: 1,
		DrainDelay:  200 * time.Millisecond,
	})

	defer worker.Close()

	select {
	case <-processed:
		hash := rdb.HGetAll(ctx, "bull:test-progress:1").Val()
		t.Logf("job hash: %v", hash)
		if _, ok := hash["progress"]; !ok {
			t.Error("expected progress field in job hash")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: job not processed")
	}
}

func TestPauseResume(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-pause", rdb)
	queue.Obliterate(ctx)

	_, _ = queue.Add(ctx, "job1", nil)

	if err := queue.Pause(ctx); err != nil {
		t.Fatalf("Pause: %v", err)
	}

	paused, err := queue.IsPaused(ctx)
	if err != nil {
		t.Fatalf("IsPaused: %v", err)
	}
	if !paused {
		t.Error("expected queue to be paused")
	}

	if err := queue.Resume(ctx); err != nil {
		t.Fatalf("Resume: %v", err)
	}

	paused, err = queue.IsPaused(ctx)
	if err != nil {
		t.Fatalf("IsPaused: %v", err)
	}
	if paused {
		t.Error("expected queue to be resumed")
	}
}

func TestGetJobCounts(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-counts", rdb)
	queue.Obliterate(ctx)

	_, _ = queue.Add(ctx, "job1", nil)
	_, _ = queue.Add(ctx, "job2", nil)

	counts, err := queue.GetCounts(ctx, bullmq.JobStateWaiting, bullmq.JobStateCompleted)
	if err != nil {
		t.Fatalf("GetCounts: %v", err)
	}

	if counts[bullmq.JobStateWaiting] != 2 {
		t.Errorf("expected 2 waiting, got %d", counts[bullmq.JobStateWaiting])
	}
}

func TestCleanJobs(t *testing.T) {
	rdb := setupRedis(t)
	ctx := context.Background()

	queue := bullmq.NewQueueWithClient("test-clean", rdb)
	queue.Obliterate(ctx)

	for i := 0; i < 2; i++ {
		_, _ = queue.Add(ctx, fmt.Sprintf("job%d", i), nil)
	}

	worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
		return nil, nil
	}, bullmq.WorkerOptions{
		Concurrency: 1,
		DrainDelay:  200 * time.Millisecond,
	})

	// Wait for jobs to complete
	time.Sleep(2 * time.Second)
	worker.Close()

	counts, _ := queue.GetCounts(ctx, bullmq.JobStateCompleted)
	if counts[bullmq.JobStateCompleted] != 2 {
		t.Fatalf("expected 2 completed, got %d", counts[bullmq.JobStateCompleted])
	}

	removed, err := queue.Clean(ctx, 0, 100, bullmq.JobStateCompleted)
	if err != nil {
		t.Fatalf("Clean: %v", err)
	}
	if len(removed) != 2 {
		t.Errorf("expected 2 cleaned, got %d", len(removed))
	}
}

func TestNewQueue(t *testing.T) {
	queue, err := bullmq.NewQueue("test-newqueue")
	if err != nil {
		t.Fatalf("NewQueue: %v", err)
	}
	if queue == nil {
		t.Fatal("expected non-nil queue")
	}
	defer queue.Close()
}

func TestNewQueueWithOptions(t *testing.T) {
	queue, err := bullmq.NewQueueWithOptions("test-opts", bullmq.QueueOptions{
		Prefix:     "myapp",
		Connection: &redis.Options{Addr: "localhost:6379"},
	})
	if err != nil {
		t.Fatalf("NewQueueWithOptions: %v", err)
	}
	if queue == nil {
		t.Fatal("expected non-nil queue")
	}
	defer queue.Close()
}
