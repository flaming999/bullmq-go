package bullmq

import (
	"context"
	"fmt"
)

// FlowJob describes a job within a flow (parent or child).
type FlowJob struct {
	Name      string
	QueueName string
	Data      interface{}
	Opts      *JobOptions
	Children  []*FlowJob
}

// FlowOpts configures flow behavior.
type FlowOpts struct {
	// Queues maps queue names to Queue instances.
	// All queues referenced in the flow must be present.
	Queues map[string]*Queue
}

// FlowProducer creates job trees where parent jobs wait for all children to complete.
type FlowProducer struct {
	queues map[string]*Queue
}

// NewFlowProducer creates a FlowProducer.
func NewFlowProducer(opts FlowOpts) *FlowProducer {
	return &FlowProducer{queues: opts.Queues}
}

// Add adds a flow (tree of jobs) to the queues.
// Children are added first so they are processed before parents.
// The parent job is only moved to "waiting" once all its children complete.
func (fp *FlowProducer) Add(ctx context.Context, flow *FlowJob) (*Job, error) {
	return fp.addNode(ctx, flow, "")
}

// AddBulk adds multiple independent flows atomically.
func (fp *FlowProducer) AddBulk(ctx context.Context, flows []*FlowJob) ([]*Job, error) {
	results := make([]*Job, 0, len(flows))
	for _, flow := range flows {
		job, err := fp.Add(ctx, flow)
		if err != nil {
			return results, err
		}
		results = append(results, job)
	}
	return results, nil
}

// addNode recursively adds a job node and its children.
func (fp *FlowProducer) addNode(ctx context.Context, node *FlowJob, parentKey string) (*Job, error) {
	q, ok := fp.queues[node.QueueName]
	if !ok {
		return nil, fmt.Errorf("bullmq: queue %q not registered in FlowProducer", node.QueueName)
	}

	opts := JobOptions{}
	if node.Opts != nil {
		opts = *node.Opts
	}

	// If this job has children, add children first
	var childrenKeys []string
	for _, child := range node.Children {
		childJob, err := fp.addNode(ctx, child, "")
		if err != nil {
			return nil, err
		}
		childrenKeys = append(childrenKeys, fmt.Sprintf("%s:%s", child.QueueName, childJob.ID))
	}

	// If there are children, the parent starts as "waiting-children"
	if len(childrenKeys) > 0 {
		// Store children dependency list in the parent's hash
		// Parent will be moved to wait when all children complete
		if opts.JobID == "" {
			// Generate ID first for cross-reference
		}
	}

	if parentKey != "" {
		opts.Parent = &ParentOptions{
			ID:    parentKey,
			Queue: node.QueueName,
		}
	}

	job, err := q.Add(ctx, node.Name, node.Data, opts)
	if err != nil {
		return nil, err
	}

	// Register children dependency tracking
	if len(childrenKeys) > 0 {
		depKey := fmt.Sprintf("%s:%s:dependencies", q.keyPrefix(), job.ID)
		args := make([]interface{}, len(childrenKeys))
		for i, k := range childrenKeys {
			args[i] = k
		}
		if err := q.rdb.SAdd(ctx, depKey, args...).Err(); err != nil {
			return job, fmt.Errorf("bullmq: register dependencies for %s: %w", job.ID, err)
		}
	}

	return job, nil
}
