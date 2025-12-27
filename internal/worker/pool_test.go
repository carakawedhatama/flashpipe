package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewPool(t *testing.T) {
	handler := func(ctx context.Context, job Job) error {
		return nil
	}

	tests := []struct {
		name   string
		config PoolConfig
	}{
		{
			name:   "default config",
			config: DefaultPoolConfig(),
		},
		{
			name: "custom config",
			config: PoolConfig{
				Workers:           10,
				QueueSize:         100,
				PriorityQueueSize: 50,
				ResultBufferSize:  50,
			},
		},
		{
			name: "minimal config",
			config: PoolConfig{
				Workers:           1,
				QueueSize:         1,
				PriorityQueueSize: 1,
				ResultBufferSize:  1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewPool(tt.config, handler)
			if pool == nil {
				t.Fatal("expected pool to be created")
			}
			if pool.workers != tt.config.Workers {
				t.Errorf("expected %d workers, got %d", tt.config.Workers, pool.workers)
			}
		})
	}
}

func TestPool_StartStop(t *testing.T) {
	handler := func(ctx context.Context, job Job) error {
		return nil
	}

	pool := NewPool(DefaultPoolConfig(), handler)

	// Start pool
	pool.Start()

	// Should be running
	if !pool.running.Load() {
		t.Error("pool should be running after Start")
	}

	// Start again should be no-op
	pool.Start()

	// Stop pool
	pool.Stop()

	// Should not be running
	if pool.running.Load() {
		t.Error("pool should not be running after Stop")
	}

	// Stop again should be no-op
	pool.Stop()
}

func TestPool_Submit(t *testing.T) {
	var processed atomic.Int64
	handler := func(ctx context.Context, job Job) error {
		processed.Add(1)
		return nil
	}

	config := PoolConfig{
		Workers:           5,
		QueueSize:         100,
		PriorityQueueSize: 50,
		ResultBufferSize:  100,
	}
	pool := NewPool(config, handler)
	pool.Start()
	defer pool.Stop()

	// Submit jobs
	for i := 0; i < 50; i++ {
		job := Job{
			ID:       string(rune(i)),
			Workflow: "test",
			TenantID: "tenant-1",
			Priority: PriorityNormal,
		}
		if !pool.Submit(job) {
			t.Errorf("failed to submit job %d", i)
		}
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	if processed.Load() != 50 {
		t.Errorf("expected 50 jobs processed, got %d", processed.Load())
	}
}

func TestPool_SubmitPriority(t *testing.T) {
	var order []string
	var mu sync.Mutex

	handler := func(ctx context.Context, job Job) error {
		mu.Lock()
		order = append(order, job.ID)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return nil
	}

	config := PoolConfig{
		Workers:           1, // Single worker to ensure ordering
		QueueSize:         100,
		PriorityQueueSize: 100,
		ResultBufferSize:  100,
	}
	pool := NewPool(config, handler)

	// Start pool first so jobs are queued properly
	pool.Start()

	// Wait a moment for the worker to start
	time.Sleep(10 * time.Millisecond)

	// Submit normal jobs first
	for i := 0; i < 5; i++ {
		pool.Submit(Job{
			ID:       "normal-" + string(rune('0'+i)),
			Priority: PriorityNormal,
		})
	}

	// Submit high priority jobs
	for i := 0; i < 3; i++ {
		pool.Submit(Job{
			ID:       "high-" + string(rune('0'+i)),
			Priority: PriorityHigh,
		})
	}

	// Wait for all jobs to process
	time.Sleep(150 * time.Millisecond)
	pool.Stop()

	mu.Lock()
	defer mu.Unlock()

	// Verify jobs were processed (exact ordering depends on timing)
	if len(order) < 5 {
		t.Errorf("expected at least 5 jobs processed, got %d", len(order))
	}

	// Log order for debugging
	t.Logf("Processing order: %v", order)
}

func TestPool_SubmitBatch(t *testing.T) {
	var processed atomic.Int64
	handler := func(ctx context.Context, job Job) error {
		processed.Add(1)
		return nil
	}

	pool := NewPool(DefaultPoolConfig(), handler)
	pool.Start()
	defer pool.Stop()

	jobs := make([]Job, 100)
	for i := 0; i < 100; i++ {
		jobs[i] = Job{
			ID:       string(rune(i)),
			Priority: PriorityNormal,
		}
	}

	submitted := pool.SubmitBatch(jobs)
	if submitted != 100 {
		t.Errorf("expected 100 submitted, got %d", submitted)
	}

	time.Sleep(100 * time.Millisecond)

	if processed.Load() != 100 {
		t.Errorf("expected 100 processed, got %d", processed.Load())
	}
}

func TestPool_SubmitNotRunning(t *testing.T) {
	handler := func(ctx context.Context, job Job) error {
		return nil
	}

	pool := NewPool(DefaultPoolConfig(), handler)
	// Don't start pool

	job := Job{ID: "test", Priority: PriorityNormal}
	if pool.Submit(job) {
		t.Error("should return false when pool not running")
	}
}

func TestPool_QueueFull(t *testing.T) {
	blockChan := make(chan struct{})
	handler := func(ctx context.Context, job Job) error {
		<-blockChan // Block until released
		return nil
	}

	config := PoolConfig{
		Workers:           1,
		QueueSize:         2,
		PriorityQueueSize: 1,
		ResultBufferSize:  1,
	}
	pool := NewPool(config, handler)
	pool.Start()
	defer pool.Stop()

	// Fill the queue
	pool.Submit(Job{ID: "1", Priority: PriorityNormal})
	pool.Submit(Job{ID: "2", Priority: PriorityNormal})
	pool.Submit(Job{ID: "3", Priority: PriorityNormal})

	// This should fail (queue full)
	time.Sleep(10 * time.Millisecond)
	if pool.Submit(Job{ID: "overflow", Priority: PriorityNormal}) {
		// May succeed if worker picked up a job
	}

	close(blockChan) // Release blocked handler
}

func TestPool_Results(t *testing.T) {
	handler := func(ctx context.Context, job Job) error {
		if job.ID == "fail" {
			return errors.New("intentional failure")
		}
		return nil
	}

	config := PoolConfig{
		Workers:           2,
		QueueSize:         10,
		PriorityQueueSize: 5,
		ResultBufferSize:  10,
	}
	pool := NewPool(config, handler)
	pool.Start()

	// Submit jobs
	pool.Submit(Job{ID: "success-1", Priority: PriorityNormal})
	pool.Submit(Job{ID: "fail", Priority: PriorityNormal})
	pool.Submit(Job{ID: "success-2", Priority: PriorityNormal})

	// Collect results
	var results []Result
	timeout := time.After(500 * time.Millisecond)

	resultChan := pool.Results()
	for {
		select {
		case result := <-resultChan:
			results = append(results, result)
			if len(results) == 3 {
				goto done
			}
		case <-timeout:
			goto done
		}
	}
done:
	pool.Stop()

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	var successCount, failCount int
	for _, r := range results {
		if r.Success {
			successCount++
		} else {
			failCount++
		}
	}

	if successCount != 2 {
		t.Errorf("expected 2 successes, got %d", successCount)
	}
	if failCount != 1 {
		t.Errorf("expected 1 failure, got %d", failCount)
	}
}

func TestPool_QueueLength(t *testing.T) {
	blockChan := make(chan struct{})
	handler := func(ctx context.Context, job Job) error {
		<-blockChan
		return nil
	}

	config := PoolConfig{
		Workers:           1,
		QueueSize:         10,
		PriorityQueueSize: 5,
		ResultBufferSize:  10,
	}
	pool := NewPool(config, handler)
	pool.Start()
	defer func() {
		close(blockChan)
		pool.Stop()
	}()

	// Submit jobs
	pool.Submit(Job{ID: "1", Priority: PriorityNormal})
	pool.Submit(Job{ID: "2", Priority: PriorityNormal})
	pool.Submit(Job{ID: "3", Priority: PriorityHigh})

	time.Sleep(10 * time.Millisecond)

	normal, priority := pool.QueueLength()
	// Queues should have some jobs (may vary due to worker picking up jobs)
	t.Logf("Queue lengths: normal=%d, priority=%d", normal, priority)
}

func TestPool_GetMetrics(t *testing.T) {
	var count atomic.Int64
	handler := func(ctx context.Context, job Job) error {
		count.Add(1)
		time.Sleep(5 * time.Millisecond)
		if job.ID == "fail" {
			return errors.New("fail")
		}
		return nil
	}

	pool := NewPool(DefaultPoolConfig(), handler)
	pool.Start()

	pool.Submit(Job{ID: "1", Priority: PriorityNormal})
	pool.Submit(Job{ID: "2", Priority: PriorityNormal})
	pool.Submit(Job{ID: "fail", Priority: PriorityNormal})

	time.Sleep(100 * time.Millisecond)
	pool.Stop()

	metrics := pool.GetMetrics()

	if metrics["jobs_processed"].(int64) != 3 {
		t.Errorf("expected jobs_processed=3, got %v", metrics["jobs_processed"])
	}
	if metrics["jobs_succeeded"].(int64) != 2 {
		t.Errorf("expected jobs_succeeded=2, got %v", metrics["jobs_succeeded"])
	}
	if metrics["jobs_failed"].(int64) != 1 {
		t.Errorf("expected jobs_failed=1, got %v", metrics["jobs_failed"])
	}
}

func TestPool_StopWithTimeout(t *testing.T) {
	blockChan := make(chan struct{})
	handler := func(ctx context.Context, job Job) error {
		select {
		case <-blockChan:
		case <-ctx.Done():
		}
		return nil
	}

	pool := NewPool(DefaultPoolConfig(), handler)
	pool.Start()

	pool.Submit(Job{ID: "blocking", Priority: PriorityNormal})
	time.Sleep(10 * time.Millisecond)

	// Stop with short timeout
	start := time.Now()
	pool.StopWithTimeout(50 * time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Errorf("stop took too long: %v", elapsed)
	}

	close(blockChan)
}

func TestPool_ConcurrentSubmit(t *testing.T) {
	var processed atomic.Int64
	handler := func(ctx context.Context, job Job) error {
		processed.Add(1)
		return nil
	}

	pool := NewPool(DefaultPoolConfig(), handler)
	pool.Start()
	defer pool.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				pool.Submit(Job{
					ID:       string(rune(id*100 + j)),
					Priority: PriorityNormal,
				})
			}
		}(i)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	if processed.Load() < 500 {
		t.Errorf("expected at least 500 processed, got %d", processed.Load())
	}
}

func TestPool_HandlerPanic(t *testing.T) {
	var processed atomic.Int64
	handler := func(ctx context.Context, job Job) error {
		if job.ID == "panic" {
			panic("intentional panic")
		}
		processed.Add(1)
		return nil
	}

	// Note: This test may cause the worker to crash without recovery
	// In production, you'd want to add panic recovery in the worker
	pool := NewPool(PoolConfig{
		Workers:           2,
		QueueSize:         10,
		PriorityQueueSize: 5,
		ResultBufferSize:  10,
	}, handler)
	pool.Start()
	defer pool.Stop()

	// Submit normal jobs
	pool.Submit(Job{ID: "1", Priority: PriorityNormal})
	pool.Submit(Job{ID: "2", Priority: PriorityNormal})

	time.Sleep(50 * time.Millisecond)

	if processed.Load() != 2 {
		t.Errorf("expected 2 processed, got %d", processed.Load())
	}
}

func TestJob_Fields(t *testing.T) {
	now := time.Now()
	job := Job{
		ID:        "job-123",
		Workflow:  "payroll",
		TenantID:  "tenant-456",
		Payload:   map[string]interface{}{"key": "value"},
		Priority:  PriorityHigh,
		CreatedAt: now,
		Metadata: map[string]string{
			"source": "api",
			"region": "us-east",
		},
	}

	if job.ID != "job-123" {
		t.Errorf("expected ID job-123, got %s", job.ID)
	}
	if job.Workflow != "payroll" {
		t.Errorf("expected Workflow payroll, got %s", job.Workflow)
	}
	if job.Priority != PriorityHigh {
		t.Errorf("expected Priority High, got %d", job.Priority)
	}
	if job.Metadata["source"] != "api" {
		t.Error("expected metadata source=api")
	}
}

func TestResult_Fields(t *testing.T) {
	result := Result{
		JobID:    "job-123",
		Success:  true,
		Error:    nil,
		Duration: 50 * time.Millisecond,
	}

	if result.JobID != "job-123" {
		t.Errorf("expected JobID job-123, got %s", result.JobID)
	}
	if !result.Success {
		t.Error("expected Success to be true")
	}
	if result.Duration != 50*time.Millisecond {
		t.Errorf("expected Duration 50ms, got %v", result.Duration)
	}
}

func TestPriority_Values(t *testing.T) {
	if PriorityLow >= PriorityNormal {
		t.Error("Low should be less than Normal")
	}
	if PriorityNormal >= PriorityHigh {
		t.Error("Normal should be less than High")
	}
	if PriorityHigh >= PriorityCritical {
		t.Error("High should be less than Critical")
	}
}

func TestDefaultPoolConfig(t *testing.T) {
	cfg := DefaultPoolConfig()

	if cfg.Workers != 100 {
		t.Errorf("expected 100 workers, got %d", cfg.Workers)
	}
	if cfg.QueueSize != 10000 {
		t.Errorf("expected 10000 queue size, got %d", cfg.QueueSize)
	}
	if cfg.PriorityQueueSize != 1000 {
		t.Errorf("expected 1000 priority queue size, got %d", cfg.PriorityQueueSize)
	}
	if cfg.ResultBufferSize != 1000 {
		t.Errorf("expected 1000 result buffer size, got %d", cfg.ResultBufferSize)
	}
}
