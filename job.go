package bullmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// JobState represents the current state of a job.
type JobState string

const (
	JobStateWaiting   JobState = "waiting"
	JobStateActive    JobState = "active"
	JobStateCompleted JobState = "completed"
	JobStateFailed    JobState = "failed"
	JobStateDelayed   JobState = "delayed"
	JobStatePaused    JobState = "paused"
	JobStateUnknown   JobState = "unknown"
)

// BackoffType determines how retry delays are calculated.
type BackoffType string

const (
	BackoffFixed       BackoffType = "fixed"
	BackoffExponential BackoffType = "exponential"
)

// BackoffOptions configures retry backoff behavior.
type BackoffOptions struct {
	Type  BackoffType `json:"type"`
	Delay int64       `json:"delay"` // milliseconds
}

// JobOptions configures job behavior when added to the queue.
type JobOptions struct {
	// Priority is a number 1-MAX_INT; lower number = higher priority.
	Priority int `json:"priority,omitempty"`

	// Delay specifies how many milliseconds to wait before the job is processed.
	Delay time.Duration `json:"delay,omitempty"`

	// Attempts is the total number of attempts to try the job until it completes.
	Attempts int `json:"attempts,omitempty"`

	// Backoff configures backoff delay between retries.
	Backoff *BackoffOptions `json:"backoff,omitempty"`

	// RemoveOnComplete removes the job from completed set after finishing.
	// If int, keeps that many jobs. If true, removes immediately.
	RemoveOnComplete int `json:"removeOnComplete,omitempty"`

	// RemoveOnFail removes the job from failed set after failing.
	RemoveOnFail int `json:"removeOnFail,omitempty"`

	// JobID allows specifying a custom job ID (useful for deduplication).
	JobID string `json:"jobId,omitempty"`

	// Repeat configures cron-based job repeating (used by Scheduler).
	Repeat *RepeatOptions `json:"repeat,omitempty"`

	// Lifo if true, adds the job to the beginning of the queue (LIFO behavior).
	Lifo bool `json:"lifo,omitempty"`

	// Timeout in milliseconds after which the job is forcefully failed.
	Timeout time.Duration `json:"timeout,omitempty"`

	// Parent configures parent-child job dependency.
	Parent *ParentOptions `json:"parent,omitempty"`
}

// RepeatOptions configures repeatable jobs.
type RepeatOptions struct {
	Cron       string        `json:"cron,omitempty"`
	Every      time.Duration `json:"every,omitempty"` // repeat every N milliseconds
	Limit      int           `json:"limit,omitempty"` // max repeats (0 = infinite)
	StartDate  *time.Time    `json:"startDate,omitempty"`
	EndDate    *time.Time    `json:"endDate,omitempty"`
	Key        string        `json:"key,omitempty"` // unique repeat key
	Count      int           `json:"count,omitempty"`
}

// ParentOptions configures parent-child job dependencies.
type ParentOptions struct {
	ID    string `json:"id"`
	Queue string `json:"queue"`
}

// Job represents a unit of work in the queue.
type Job struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Data        interface{}    `json:"data"`
	Opts        JobOptions     `json:"opts"`
	Progress    interface{}    `json:"progress"`
	Attempts    int            `json:"attempts"`    // current attempt count
	AttemptsMade int           `json:"attemptsMade"`
	Timestamp   int64          `json:"timestamp"`   // creation time (ms)
	ProcessedOn int64          `json:"processedOn"` // when processing started (ms)
	FinishedOn  int64          `json:"finishedOn"`  // when processing finished (ms)
	ReturnValue interface{}    `json:"returnvalue"`
	FailedReason string        `json:"failedReason"`
	Stacktrace  []string       `json:"stacktrace"`
	ParentKey   string         `json:"parentKey,omitempty"`
	RepeatJobKey string        `json:"repeatJobKey,omitempty"`

	// internal
	queueName string
	rdb       redis.UniversalClient
}

// UpdateProgress updates the job's progress value and publishes a progress event.
func (j *Job) UpdateProgress(ctx context.Context, progress interface{}) error {
	j.Progress = progress
	data, err := json.Marshal(progress)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s:%s", j.queueName, j.ID)
	pipe := j.rdb.Pipeline()
	pipe.HSet(ctx, key, "progress", string(data))
	pipe.Publish(ctx, fmt.Sprintf("%s:events", j.queueName), mustMarshal(map[string]interface{}{
		"event":    "progress",
		"jobId":    j.ID,
		"data":     progress,
		"ts":       time.Now().UnixMilli(),
	}))
	_, err = pipe.Exec(ctx)
	return err
}

// Log appends a log entry to the job's log.
func (j *Job) Log(ctx context.Context, msg string) error {
	key := fmt.Sprintf("%s:%s:logs", j.queueName, j.ID)
	return j.rdb.RPush(ctx, key, msg).Err()
}

// GetLogs returns paginated logs for this job.
func (j *Job) GetLogs(ctx context.Context, start, end int64) ([]string, error) {
	key := fmt.Sprintf("%s:%s:logs", j.queueName, j.ID)
	return j.rdb.LRange(ctx, key, start, end).Result()
}

// toHash serializes a Job to a Redis hash map.
func (j *Job) toHash() map[string]interface{} {
	dataJSON, _ := json.Marshal(j.Data)
	optsJSON, _ := json.Marshal(j.Opts)
	progressJSON, _ := json.Marshal(j.Progress)

	return map[string]interface{}{
		"id":           j.ID,
		"name":         j.Name,
		"data":         string(dataJSON),
		"opts":         string(optsJSON),
		"progress":     string(progressJSON),
		"attempts":     j.Attempts,
		"attemptsMade": j.AttemptsMade,
		"timestamp":    j.Timestamp,
		"processedOn":  j.ProcessedOn,
		"finishedOn":   j.FinishedOn,
		"returnvalue":  mustMarshal(j.ReturnValue),
		"failedReason": j.FailedReason,
		"parentKey":    j.ParentKey,
		"repeatJobKey": j.RepeatJobKey,
	}
}

// fromHash deserializes a Redis hash map into a Job.
func jobFromHash(hash map[string]string, queueName string, rdb redis.UniversalClient) (*Job, error) {
	j := &Job{
		queueName: queueName,
		rdb:       rdb,
	}

	j.ID = hash["id"]
	j.Name = hash["name"]
	j.FailedReason = hash["failedReason"]
	j.ParentKey = hash["parentKey"]
	j.RepeatJobKey = hash["repeatJobKey"]

	if v, ok := hash["data"]; ok && v != "" {
		if err := json.Unmarshal([]byte(v), &j.Data); err != nil {
			j.Data = v
		}
	}
	if v, ok := hash["opts"]; ok && v != "" {
		_ = json.Unmarshal([]byte(v), &j.Opts)
	}
	if v, ok := hash["progress"]; ok && v != "" {
		_ = json.Unmarshal([]byte(v), &j.Progress)
	}
	if v, ok := hash["returnvalue"]; ok && v != "" {
		_ = json.Unmarshal([]byte(v), &j.ReturnValue)
	}

	j.Attempts = parseInt(hash["attempts"])
	j.AttemptsMade = parseInt(hash["attemptsMade"])
	j.Timestamp = parseInt64(hash["timestamp"])
	j.ProcessedOn = parseInt64(hash["processedOn"])
	j.FinishedOn = parseInt64(hash["finishedOn"])

	return j, nil
}

func parseInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func parseInt64(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

func mustMarshal(v interface{}) string {
	if v == nil {
		return ""
	}
	b, _ := json.Marshal(v)
	return string(b)
}
