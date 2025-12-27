package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Job represents a unit of work to be processed.
type Job struct {
	ID        string
	Workflow  string // payroll, attendance, trial_balance, etc.
	TenantID  string
	Payload   interface{}
	Priority  Priority
	CreatedAt time.Time
	Metadata  map[string]string
}

// Priority defines job priority levels.
type Priority int

const (
	PriorityLow Priority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

// Result represents the outcome of a job.
type Result struct {
	JobID    string
	Success  bool
	Error    error
	Duration time.Duration
}

// Handler processes a job and returns a result.
type Handler func(ctx context.Context, job Job) error

// Pool is a high-throughput worker pool with priority queues.
type Pool struct {
	workers      int
	handler      Handler
	jobs         chan Job
	priorityJobs chan Job // High priority queue
	results      chan Result
	wg           sync.WaitGroup
	ctx          context.Context
	cancel       context.CancelFunc
	running      atomic.Bool
	metrics      *PoolMetrics
}

// PoolMetrics tracks worker pool performance.
type PoolMetrics struct {
	JobsProcessed  atomic.Int64
	JobsSucceeded  atomic.Int64
	JobsFailed     atomic.Int64
	JobsQueued     atomic.Int64
	ProcessingTime atomic.Int64 // Total processing time in nanoseconds
	ActiveWorkers  atomic.Int32
}

// PoolConfig holds configuration for the worker pool.
type PoolConfig struct {
	Workers           int
	QueueSize         int
	PriorityQueueSize int
	ResultBufferSize  int
}

// DefaultPoolConfig returns sensible defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		Workers:           100,
		QueueSize:         10000,
		PriorityQueueSize: 1000,
		ResultBufferSize:  1000,
	}
}

// NewPool creates a new worker pool.
func NewPool(cfg PoolConfig, handler Handler) *Pool {
	ctx, cancel := context.WithCancel(context.Background())

	return &Pool{
		workers:      cfg.Workers,
		handler:      handler,
		jobs:         make(chan Job, cfg.QueueSize),
		priorityJobs: make(chan Job, cfg.PriorityQueueSize),
		results:      make(chan Result, cfg.ResultBufferSize),
		ctx:          ctx,
		cancel:       cancel,
		metrics:      &PoolMetrics{},
	}
}

// Start starts the worker pool.
func (p *Pool) Start() {
	if p.running.Swap(true) {
		return // Already running
	}

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}
}

// worker is the main worker loop.
func (p *Pool) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return

		// Priority queue takes precedence
		case job := <-p.priorityJobs:
			p.processJob(job)

		default:
			// Check priority queue first, then regular queue
			select {
			case <-p.ctx.Done():
				return
			case job := <-p.priorityJobs:
				p.processJob(job)
			case job := <-p.jobs:
				p.processJob(job)
			}
		}
	}
}

// processJob handles a single job.
func (p *Pool) processJob(job Job) {
	p.metrics.ActiveWorkers.Add(1)
	defer p.metrics.ActiveWorkers.Add(-1)

	start := time.Now()

	err := p.handler(p.ctx, job)

	duration := time.Since(start)
	p.metrics.ProcessingTime.Add(duration.Nanoseconds())
	p.metrics.JobsProcessed.Add(1)

	result := Result{
		JobID:    job.ID,
		Duration: duration,
	}

	if err != nil {
		result.Success = false
		result.Error = err
		p.metrics.JobsFailed.Add(1)
	} else {
		result.Success = true
		p.metrics.JobsSucceeded.Add(1)
	}

	// Non-blocking send to results channel
	select {
	case p.results <- result:
	default:
		// Results buffer full, drop result (metrics still tracked)
	}
}

// Submit submits a job to the pool.
func (p *Pool) Submit(job Job) bool {
	if !p.running.Load() {
		return false
	}

	p.metrics.JobsQueued.Add(1)

	if job.Priority >= PriorityHigh {
		select {
		case p.priorityJobs <- job:
			return true
		default:
			// Priority queue full, try regular queue
		}
	}

	select {
	case p.jobs <- job:
		return true
	default:
		p.metrics.JobsQueued.Add(-1)
		return false // Queue full
	}
}

// SubmitBatch submits multiple jobs.
func (p *Pool) SubmitBatch(jobs []Job) int {
	submitted := 0
	for _, job := range jobs {
		if p.Submit(job) {
			submitted++
		}
	}
	return submitted
}

// Results returns the results channel for consuming job outcomes.
func (p *Pool) Results() <-chan Result {
	return p.results
}

// Stop gracefully stops the worker pool.
func (p *Pool) Stop() {
	if !p.running.Swap(false) {
		return // Already stopped
	}

	p.cancel()
	p.wg.Wait()
	close(p.results)
}

// StopWithTimeout stops the pool with a timeout for graceful shutdown.
func (p *Pool) StopWithTimeout(timeout time.Duration) {
	if !p.running.Swap(false) {
		return
	}

	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}

	close(p.results)
}

// QueueLength returns the current queue lengths.
func (p *Pool) QueueLength() (normal, priority int) {
	return len(p.jobs), len(p.priorityJobs)
}

// GetMetrics returns current pool metrics.
func (p *Pool) GetMetrics() map[string]interface{} {
	processed := p.metrics.JobsProcessed.Load()
	var avgProcessingTime float64
	if processed > 0 {
		avgProcessingTime = float64(p.metrics.ProcessingTime.Load()) / float64(processed) / float64(time.Millisecond)
	}

	return map[string]interface{}{
		"jobs_processed":         processed,
		"jobs_succeeded":         p.metrics.JobsSucceeded.Load(),
		"jobs_failed":            p.metrics.JobsFailed.Load(),
		"jobs_queued":            p.metrics.JobsQueued.Load(),
		"active_workers":         p.metrics.ActiveWorkers.Load(),
		"avg_processing_time_ms": avgProcessingTime,
		"queue_length":           len(p.jobs),
		"priority_queue_length":  len(p.priorityJobs),
	}
}
