package bullmq

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/redis/go-redis/v9"
)

// EventType represents a queue or job lifecycle event.
type EventType string

const (
	EventWaiting   EventType = "waiting"
	EventActive    EventType = "active"
	EventCompleted EventType = "completed"
	EventFailed    EventType = "failed"
	EventDelayed   EventType = "delayed"
	EventProgress  EventType = "progress"
	EventDrained   EventType = "drained"
	EventPaused    EventType = "paused"
	EventResumed   EventType = "resumed"
	EventStalled   EventType = "stalled"
	EventError     EventType = "error"
	EventAdded     EventType = "added"
	EventRemoved   EventType = "removed"
)

// Event carries data about a queue lifecycle event.
type Event struct {
	Type         EventType   `json:"event"`
	JobID        string      `json:"jobId,omitempty"`
	Data         interface{} `json:"data,omitempty"`
	ReturnValue  interface{} `json:"returnvalue,omitempty"`
	FailedReason string      `json:"failedReason,omitempty"`
	Delay        int64       `json:"delay,omitempty"`
	Prev         string      `json:"prev,omitempty"`
	Timestamp    int64       `json:"ts,omitempty"`
}

// EventHandler is a function that handles an event.
type EventHandler func(event *Event)

// EventEmitter manages event subscriptions.
type EventEmitter struct {
	mu       sync.RWMutex
	handlers map[EventType][]EventHandler
	allHandlers []EventHandler
}

// NewEventEmitter creates a new EventEmitter.
func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		handlers: make(map[EventType][]EventHandler),
	}
}

// On registers a handler for a specific event type.
func (e *EventEmitter) On(eventType EventType, handler EventHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handlers[eventType] = append(e.handlers[eventType], handler)
}

// OnAny registers a handler for all event types.
func (e *EventEmitter) OnAny(handler EventHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.allHandlers = append(e.allHandlers, handler)
}

// Off removes all handlers for a specific event type.
func (e *EventEmitter) Off(eventType EventType) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.handlers, eventType)
}

// emit dispatches an event to all registered handlers.
func (e *EventEmitter) emit(event *Event) {
	e.mu.RLock()
	handlers := append([]EventHandler{}, e.handlers[event.Type]...)
	allH := append([]EventHandler{}, e.allHandlers...)
	e.mu.RUnlock()

	for _, h := range handlers {
		go h(event)
	}
	for _, h := range allH {
		go h(event)
	}
}

// QueueEvents subscribes to queue events via Redis Pub/Sub.
// This allows multiple processes to receive real-time events.
type QueueEvents struct {
	*EventEmitter
	rdb       redis.UniversalClient
	queueName string
	pubsub    *redis.PubSub
	cancel    context.CancelFunc
}

// NewQueueEvents creates a new QueueEvents subscriber.
func NewQueueEvents(rdb redis.UniversalClient, queueName string) *QueueEvents {
	return &QueueEvents{
		EventEmitter: NewEventEmitter(),
		rdb:          rdb,
		queueName:    queueName,
	}
}

// Start begins listening for queue events.
func (qe *QueueEvents) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	qe.cancel = cancel

	channel := qe.queueName + ":events"
	qe.pubsub = qe.rdb.Subscribe(ctx, channel)

	go func() {
		defer qe.pubsub.Close()
		ch := qe.pubsub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				var event Event
				if err := json.Unmarshal([]byte(msg.Payload), &event); err == nil {
					qe.emit(&event)
				}
			}
		}
	}()

	return nil
}

// Close stops the event listener.
func (qe *QueueEvents) Close() error {
	if qe.cancel != nil {
		qe.cancel()
	}
	if qe.pubsub != nil {
		return qe.pubsub.Close()
	}
	return nil
}
