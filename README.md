# BullMQ — Go Edition

A Redis-backed message queue for Go, inspired by [BullMQ](https://docs.bullmq.io).

## Features

- ✅ **FIFO / LIFO queues** — standard job ordering
- ✅ **Priority queues** — lower number = higher priority
- ✅ **Delayed jobs** — process after a time delay
- ✅ **Concurrency control** — configurable workers per queue
- ✅ **Automatic retries** — with fixed or exponential backoff
- ✅ **Job lifecycle events** — via Redis Pub/Sub across processes
- ✅ **Job Scheduler** — cron and interval-based repeatable jobs
- ✅ **Parent–child flows** — jobs that wait for children to complete
- ✅ **Stall detection** — recover crashed worker jobs automatically
- ✅ **Rate limiting** — cap jobs processed per time window
- ✅ **Atomic operations** — Lua scripts guarantee race-free transitions
- ✅ **Job progress & logs** — update progress and append log entries
- ✅ **Bulk add** — add many jobs in a single call
- ✅ **Queue management** — pause, resume, drain, clean, obliterate

---

## Installation

```bash
go get github.com/flaming999/bullmq-go
```

**Dependencies:**
- `github.com/redis/go-redis/v9` — Redis client
- `github.com/robfig/cron/v3` — cron scheduling
- `github.com/google/uuid` — unique ID generation

---

## Quick Start

```go
package main

import (
    "context"
    "fmt"

    "github.com/flaming999/bullmq-go"
)

func main() {
    ctx := context.Background()

    queue, _ := bullmq.NewQueue("my-queue")

    queue.Add(ctx, "email", map[string]string{"to": "user@example.com"})

    worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
        fmt.Println("send to:", job.Data)
        return "ok", nil
    }, bullmq.WorkerOptions{Concurrency: 5})

    defer worker.Close()
    select {}
}
```

## Quick Start (with Redis client)

```go
package main

import (
    "context"
    "fmt"

    "github.com/redis/go-redis/v9"
    "github.com/flaming999/bullmq-go"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
    ctx := context.Background()

    // Create a queue
    queue := bullmq.NewQueueWithClient("my-queue", rdb)

    // Add a job
    job, _ := queue.Add(ctx, "send-email", map[string]string{
        "to": "user@example.com",
    })
    fmt.Println("Added job:", job.ID)

    // Create a worker to process jobs
    worker := bullmq.NewWorker(queue, func(ctx context.Context, job *bullmq.Job) (interface{}, error) {
        fmt.Println("Processing:", job.Data)
        return "done", nil
    }, bullmq.WorkerOptions{Concurrency: 5})

    defer worker.Close()

    // Block forever (or until signal)
    select {}
}
```

---

## Architecture

### Redis Data Model

Each queue uses a set of Redis keys under the `{prefix}:{queue}` namespace:

| Key                        | Type    | Purpose                              |
|----------------------------|---------|--------------------------------------|
| `bull:q:wait`              | List    | FIFO waiting jobs                    |
| `bull:q:active`            | List    | Currently processing jobs            |
| `bull:q:completed`         | ZSet    | Completed jobs (score = timestamp)   |
| `bull:q:failed`            | ZSet    | Failed jobs (score = timestamp)      |
| `bull:q:delayed`           | ZSet    | Delayed jobs (score = processAt)     |
| `bull:q:priority`          | ZSet    | Priority queue (score = priority)    |
| `bull:q:stalled`           | Set     | Active jobs tracked for stall check  |
| `bull:q:{id}`              | Hash    | Job data and metadata                |
| `bull:q:id`                | String  | Auto-increment job ID counter        |
| `bull:q:events`            | PubSub  | Real-time event channel              |
| `bull:q:repeat`            | ZSet    | Repeatable job registry              |

### Job State Machine

```
        ┌──────────┐
        │ waiting  │◄─────────────────────┐
        └────┬─────┘                      │ retry
             │ moveToActive               │
        ┌────▼─────┐                      │
        │  active  │──────► failed ───────┘
        └────┬─────┘          ▲
             │ complete       │ error / timeout
        ┌────▼─────┐          │
        │completed │          │
        └──────────┘          │
                              │
        ┌──────────┐          │
   add  │ delayed  │──────────┘ (after delay expires → waiting)
 ──────►└──────────┘
```

### Atomic Lua Scripts

All state transitions use Lua scripts loaded into Redis. This means:
- No TOCTOU race conditions between workers
- Safe horizontal scaling (many workers, many servers)
- Efficient: single round-trip per transition

---

## API Reference

### Queue

```go
// Create
queue, err := bullmq.NewQueue(name)                                // uses defaults
queue, err := bullmq.NewQueueWithOptions(name, bullmq.QueueOptions{}) // with custom options

queue := bullmq.NewQueueWithClient(name, rdb)

// Add jobs
job, err := queue.Add(ctx, name, data)
job, err := queue.Add(ctx, name, data, jobOpts)
jobs, err := queue.AddBulk(ctx, []bullmq.BulkJob{{Name, Data, Opts}})

// Inspect
job, err := queue.GetJob(ctx, id)
jobs, err := queue.GetJobs(ctx, state, start, end, ascending)
state, err := queue.GetJobState(ctx, id)
counts, err := queue.GetCounts(ctx, states...)

// Manage
err = queue.Pause(ctx)
err = queue.Resume(ctx)
err = queue.RetryJob(ctx, id)
err = queue.RetryJobs(ctx)
err = queue.RemoveJob(ctx, id)
removed, err = queue.Clean(ctx, grace, limit, state)
err = queue.Drain(ctx, includeDelayed)
err = queue.Obliterate(ctx)   // ⚠ deletes everything
```

### JobOptions

```go
bullmq.JobOptions{
    Priority:         1,                    // 1 = highest, higher = lower
    Delay:            5 * time.Second,      // process after delay
    Attempts:         3,                    // max attempts (including first)
    Backoff: &bullmq.BackoffOptions{
        Type:  bullmq.BackoffExponential,   // or BackoffFixed
        Delay: 1000,                        // ms base delay
    },
    RemoveOnComplete: 100,                  // keep last N completed jobs
    RemoveOnFail:     50,                   // keep last N failed jobs
    JobID:            "my-unique-id",       // custom ID (deduplication)
    Lifo:             false,                // true = LIFO ordering
    Timeout:          30 * time.Second,     // fail job if it runs > Timeout
}
```

### Worker

```go
worker := bullmq.NewWorker(queue, processor, bullmq.WorkerOptions{
    Concurrency:     10,
    LockDuration:    30 * time.Second,
    StalledInterval: 30 * time.Second,
    MaxStalledCount: 1,
    DrainDelay:      5 * time.Second,
    Limiter: &bullmq.RateLimiterOptions{
        Max:      100,
        Duration: time.Minute,
    },
    AutoRun: ptr(false), // default: true (nil = auto start)
})

// Lifecycle
worker.Run()     // start manually when AutoRun=false
worker.Close()   // graceful shutdown
worker.IsRunning()
worker.Concurrency()

// Events
worker.EventEmitter.On(bullmq.EventCompleted, handler)
worker.EventEmitter.On(bullmq.EventFailed, handler)
worker.EventEmitter.On(bullmq.EventProgress, handler)
worker.EventEmitter.OnAny(handler) // all events
```

### Job (inside processor)

```go
func processor(ctx context.Context, job *bullmq.Job) (interface{}, error) {
    // Job fields
    job.ID
    job.Name
    job.Data           // any JSON-serializable value
    job.Opts           // JobOptions used when adding
    job.AttemptsMade   // how many times this job has been attempted
    job.Timestamp      // creation time (Unix ms)
    job.ProcessedOn    // when this attempt started

    // Report progress (0–100 or any value)
    job.UpdateProgress(ctx, 42)

    // Append to job log
    job.Log(ctx, "processing step 1")

    // Read logs
    logs, _ := job.GetLogs(ctx, 0, -1)

    // Return value is stored and emitted in completed event
    return map[string]string{"result": "ok"}, nil
}
```

### Job Scheduler

```go
scheduler := bullmq.NewJobScheduler(queue, bullmq.SchedulerOptions{})
scheduler.Start()

// Cron expression (6-field with seconds)
scheduler.Upsert(ctx, "report", data, bullmq.JobOptions{
    Repeat: &bullmq.RepeatOptions{
        Cron:  "0 9 * * 1-5", // 9am Mon-Fri
        Limit: 0,             // infinite
    },
})

// Fixed interval
scheduler.Upsert(ctx, "heartbeat", nil, bullmq.JobOptions{
    Repeat: &bullmq.RepeatOptions{
        Every:   30 * time.Second,
        Limit:   100,
        EndDate: &someTime,
    },
})

jobs := scheduler.GetRepeatableJobs(ctx)
scheduler.Remove(ctx, key)

stopCtx := scheduler.Stop()
<-stopCtx.Done()
```

### Queue Events (cross-process)

```go
// Subscribe to events emitted by any worker on any machine
queueEvents := bullmq.NewQueueEvents(rdb, "bull:my-queue")

queueEvents.On(bullmq.EventCompleted, func(e *bullmq.Event) {
    fmt.Println("Job completed:", e.JobID, "result:", e.Data)
})
queueEvents.On(bullmq.EventFailed, func(e *bullmq.Event) {
    fmt.Println("Job failed:", e.JobID, "reason:", e.FailedReason)
})
queueEvents.On(bullmq.EventProgress, func(e *bullmq.Event) {
    fmt.Println("Progress:", e.JobID, e.Data)
})
queueEvents.OnAny(func(e *bullmq.Event) {
    fmt.Println("Event:", e.Type, e.JobID)
})

queueEvents.Start(ctx)
defer queueEvents.Close()
```

### Flows (Parent-Child Dependencies)

```go
flow := bullmq.NewFlowProducer(bullmq.FlowOpts{
    Queues: map[string]*bullmq.Queue{
        "parent-q": parentQueue,
        "child-q":  childQueue,
    },
})

parent, err := flow.Add(ctx, &bullmq.FlowJob{
    Name:      "process-order",
    QueueName: "parent-q",
    Data:      orderData,
    Children: []*bullmq.FlowJob{
        {Name: "validate", QueueName: "child-q", Data: step1Data},
        {Name: "charge",   QueueName: "child-q", Data: step2Data},
        {Name: "notify",   QueueName: "child-q", Data: step3Data},
    },
})
```

---

## Event Types

| Event       | When                                      |
|-------------|-------------------------------------------|
| `waiting`   | Job added to the wait list                |
| `active`    | Job picked up by a worker                 |
| `completed` | Job finished successfully                 |
| `failed`    | Job failed (all retries exhausted)        |
| `delayed`   | Job added with a delay or retrying later  |
| `progress`  | Job called `UpdateProgress()`             |
| `drained`   | Queue has no more waiting jobs            |
| `paused`    | Queue was paused                          |
| `resumed`   | Queue was resumed                         |
| `stalled`   | Job detected as stalled and re-queued     |
| `error`     | Internal error in worker                  |
| `added`     | Job successfully added                    |
| `removed`   | Job removed                               |

---

## Backoff Strategies

```go
// Fixed: always wait the same amount
&bullmq.BackoffOptions{Type: bullmq.BackoffFixed, Delay: 5000} // 5s every retry

// Exponential: delay * 2^(attempt-1), capped at 1 hour
// Attempt 1: 1s, Attempt 2: 2s, Attempt 3: 4s, ...
&bullmq.BackoffOptions{Type: bullmq.BackoffExponential, Delay: 1000}
```

---

## Concurrency & Scaling

- Run **multiple workers** in the same process using different `Concurrency` values.
- Run **multiple processes** on different machines — all sharing the same Redis.
- Lua scripts ensure only one worker processes each job, even under high concurrency.
- Stall detection recovers jobs whose worker crashed before completing.

---

## License

Apache License
