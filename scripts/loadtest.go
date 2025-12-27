// Package main provides a high-performance load testing tool for FlashPipe.
// It generates realistic ERP event data and measures throughput, latency, and error rates.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Configuration
var (
	baseURL        = flag.String("url", "http://localhost:8181", "FlashPipe base URL")
	duration       = flag.Duration("duration", 60*time.Second, "Test duration")
	concurrency    = flag.Int("concurrency", 100, "Number of concurrent workers")
	batchSize      = flag.Int("batch-size", 100, "Events per batch request")
	rateLimit      = flag.Int("rate", 0, "Requests per second (0 = unlimited)")
	mode           = flag.String("mode", "mixed", "Test mode: single, batch, async, mixed")
	tenants        = flag.Int("tenants", 10, "Number of simulated tenants")
	warmupDuration = flag.Duration("warmup", 5*time.Second, "Warmup duration before measuring")
)

// Metrics
type Metrics struct {
	RequestsSent    atomic.Int64
	RequestsSuccess atomic.Int64
	RequestsFailed  atomic.Int64
	EventsSent      atomic.Int64
	BytesSent       atomic.Int64
	TotalLatencyNs  atomic.Int64
	MinLatencyNs    atomic.Int64
	MaxLatencyNs    atomic.Int64
	StatusCodes     sync.Map
	Errors          sync.Map
}

var metrics = &Metrics{}

// Event templates
var workflows = []string{"payroll", "attendance", "trial_balance", "general"}
var priorities = []string{"low", "normal", "high", "critical"}
var eventTypes = map[string][]string{
	"payroll": {
		"salary.calculated", "salary.approved", "salary.paid",
		"bonus.calculated", "deduction.applied", "tax.calculated",
	},
	"attendance": {
		"check_in", "check_out", "break.start", "break.end",
		"leave.requested", "leave.approved", "overtime.logged",
	},
	"trial_balance": {
		"entry.created", "entry.approved", "report.generated",
		"period.closed", "reconciliation.completed",
	},
	"general": {
		"event.created", "event.updated", "event.deleted",
		"notification.sent", "audit.logged",
	},
}

// HTTP client with connection pooling
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        500,
		MaxIdleConnsPerHost: 500,
		MaxConnsPerHost:     500,
		IdleConnTimeout:     90 * time.Second,
	},
}

func main() {
	flag.Parse()

	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║           FlashPipe Load Testing Tool                          ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ URL:         %-50s ║\n", *baseURL)
	fmt.Printf("║ Duration:    %-50s ║\n", *duration)
	fmt.Printf("║ Concurrency: %-50d ║\n", *concurrency)
	fmt.Printf("║ Batch Size:  %-50d ║\n", *batchSize)
	fmt.Printf("║ Mode:        %-50s ║\n", *mode)
	fmt.Printf("║ Tenants:     %-50d ║\n", *tenants)
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Initialize min latency to max value
	metrics.MinLatencyNs.Store(int64(^uint64(0) >> 1))

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Check server health
	if !checkHealth() {
		fmt.Println("❌ Server health check failed. Is FlashPipe running?")
		os.Exit(1)
	}
	fmt.Println("✅ Server health check passed")

	// Warmup phase
	fmt.Printf("\n🔥 Warming up for %s...\n", *warmupDuration)
	runLoadTest(*warmupDuration, true)

	// Reset metrics for actual test
	metrics = &Metrics{}
	metrics.MinLatencyNs.Store(int64(^uint64(0) >> 1))

	// Start actual load test
	fmt.Printf("\n🚀 Starting load test for %s...\n\n", *duration)

	done := make(chan struct{})
	go func() {
		runLoadTest(*duration, false)
		close(done)
	}()

	// Print live stats
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	startTime := time.Now()

loop:
	for {
		select {
		case <-ticker.C:
			printLiveStats(time.Since(startTime))
		case <-sigChan:
			fmt.Println("\n\n⚠️  Interrupted! Printing final stats...")
			break loop
		case <-done:
			break loop
		}
	}

	// Print final results
	printFinalResults(time.Since(startTime))
}

func checkHealth() bool {
	resp, err := httpClient.Get(*baseURL + "/health/live")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func runLoadTest(testDuration time.Duration, warmup bool) {
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// Rate limiter
	var rateLimiter <-chan time.Time
	if *rateLimit > 0 {
		rateLimiter = time.Tick(time.Second / time.Duration(*rateLimit))
	}

	// Start workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(workerID, stopChan, rateLimiter, warmup)
		}(i)
	}

	// Wait for duration
	time.Sleep(testDuration)
	close(stopChan)
	wg.Wait()
}

func worker(id int, stop <-chan struct{}, rateLimiter <-chan time.Time, warmup bool) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for {
		select {
		case <-stop:
			return
		default:
		}

		if rateLimiter != nil {
			<-rateLimiter
		}

		// Choose request type based on mode
		switch *mode {
		case "single":
			sendSingleEvent(rng, warmup)
		case "batch":
			sendBatchEvents(rng, warmup)
		case "async":
			sendAsyncEvent(rng, warmup)
		case "mixed":
			r := rng.Float32()
			if r < 0.3 {
				sendSingleEvent(rng, warmup)
			} else if r < 0.7 {
				sendBatchEvents(rng, warmup)
			} else {
				sendAsyncEvent(rng, warmup)
			}
		}
	}
}

func sendSingleEvent(rng *rand.Rand, warmup bool) {
	event := generateEvent(rng)
	body, _ := json.Marshal(event)

	start := time.Now()
	resp, err := doRequest("POST", "/api/v1/events", body, event["tenant_id"].(string))
	latency := time.Since(start)

	if !warmup {
		recordMetrics(resp, err, latency, 1, len(body))
	}
}

func sendBatchEvents(rng *rand.Rand, warmup bool) {
	tenantID := fmt.Sprintf("tenant-%d", rng.Intn(*tenants))
	workflow := workflows[rng.Intn(len(workflows))]

	events := make([]map[string]interface{}, *batchSize)
	for i := 0; i < *batchSize; i++ {
		events[i] = generateEventForTenant(rng, tenantID, workflow)
	}

	batch := map[string]interface{}{
		"idempotency_key": fmt.Sprintf("batch-%d-%d", time.Now().UnixNano(), rng.Int()),
		"tenant_id":       tenantID,
		"workflow":        workflow,
		"events":          events,
		"priority":        priorities[rng.Intn(len(priorities))],
	}

	body, _ := json.Marshal(batch)

	start := time.Now()
	resp, err := doRequest("POST", "/api/v1/events/batch", body, tenantID)
	latency := time.Since(start)

	if !warmup {
		recordMetrics(resp, err, latency, *batchSize, len(body))
	}
}

func sendAsyncEvent(rng *rand.Rand, warmup bool) {
	event := generateEvent(rng)
	body, _ := json.Marshal(event)

	start := time.Now()
	resp, err := doRequest("POST", "/api/v1/events/async", body, event["tenant_id"].(string))
	latency := time.Since(start)

	if !warmup {
		recordMetrics(resp, err, latency, 1, len(body))
	}
}

func generateEvent(rng *rand.Rand) map[string]interface{} {
	tenantID := fmt.Sprintf("tenant-%d", rng.Intn(*tenants))
	workflow := workflows[rng.Intn(len(workflows))]
	return generateEventForTenant(rng, tenantID, workflow)
}

func generateEventForTenant(rng *rand.Rand, tenantID, workflow string) map[string]interface{} {
	eventTypes := eventTypes[workflow]
	eventType := eventTypes[rng.Intn(len(eventTypes))]

	return map[string]interface{}{
		"idempotency_key": fmt.Sprintf("evt-%d-%d", time.Now().UnixNano(), rng.Int()),
		"tenant_id":       tenantID,
		"user_id":         fmt.Sprintf("user-%d", rng.Intn(1000)),
		"workflow":        workflow,
		"event_type":      eventType,
		"priority":        priorities[rng.Intn(len(priorities))],
		"timestamp":       time.Now().UTC().Format(time.RFC3339),
		"payload":         generatePayload(rng, workflow),
		"metadata": map[string]interface{}{
			"correlation_id": fmt.Sprintf("corr-%d", rng.Int()),
			"source":         "loadtest",
			"version":        "1.0.0",
		},
	}
}

func generatePayload(rng *rand.Rand, workflow string) map[string]interface{} {
	switch workflow {
	case "payroll":
		return map[string]interface{}{
			"employee_id":    fmt.Sprintf("EMP-%05d", rng.Intn(10000)),
			"period_start":   time.Now().AddDate(0, -1, 0).Format("2006-01-02"),
			"period_end":     time.Now().Format("2006-01-02"),
			"base_salary":    float64(rng.Intn(10000000) + 1000000),
			"allowances":     float64(rng.Intn(1000000)),
			"deductions":     float64(rng.Intn(500000)),
			"overtime_hours": float64(rng.Intn(50)),
			"overtime_rate":  float64(rng.Intn(100000) + 10000),
			"currency":       "IDR",
		}
	case "attendance":
		return map[string]interface{}{
			"employee_id":   fmt.Sprintf("EMP-%05d", rng.Intn(10000)),
			"date":          time.Now().Format("2006-01-02"),
			"check_in":      time.Now().Add(-8 * time.Hour).Format(time.RFC3339),
			"check_out":     time.Now().Format(time.RFC3339),
			"break_minutes": rng.Intn(60),
			"status":        []string{"present", "late", "absent"}[rng.Intn(3)],
			"location":      fmt.Sprintf("Office-%d", rng.Intn(10)),
			"device_id":     fmt.Sprintf("DEV-%04d", rng.Intn(100)),
		}
	case "trial_balance":
		return map[string]interface{}{
			"account_code": fmt.Sprintf("%04d", rng.Intn(9999)+1000),
			"account_name": fmt.Sprintf("Account %d", rng.Intn(100)),
			"debit":        float64(rng.Intn(100000000)),
			"credit":       float64(rng.Intn(100000000)),
			"currency":     "IDR",
		}
	default:
		return map[string]interface{}{
			"key":   fmt.Sprintf("value-%d", rng.Int()),
			"count": rng.Intn(1000),
		}
	}
}

func doRequest(method, path string, body []byte, tenantID string) (*http.Response, error) {
	req, err := http.NewRequest(method, *baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", tenantID)
	req.Header.Set("X-Request-ID", fmt.Sprintf("loadtest-%d", time.Now().UnixNano()))

	return httpClient.Do(req)
}

func recordMetrics(resp *http.Response, err error, latency time.Duration, events int, bytes int) {
	metrics.RequestsSent.Add(1)
	metrics.EventsSent.Add(int64(events))
	metrics.BytesSent.Add(int64(bytes))
	metrics.TotalLatencyNs.Add(latency.Nanoseconds())

	// Update min/max latency atomically
	for {
		min := metrics.MinLatencyNs.Load()
		if latency.Nanoseconds() >= min || metrics.MinLatencyNs.CompareAndSwap(min, latency.Nanoseconds()) {
			break
		}
	}
	for {
		max := metrics.MaxLatencyNs.Load()
		if latency.Nanoseconds() <= max || metrics.MaxLatencyNs.CompareAndSwap(max, latency.Nanoseconds()) {
			break
		}
	}

	if err != nil {
		metrics.RequestsFailed.Add(1)
		count, _ := metrics.Errors.LoadOrStore(err.Error(), new(atomic.Int64))
		count.(*atomic.Int64).Add(1)
		return
	}

	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		metrics.RequestsSuccess.Add(1)
	} else {
		metrics.RequestsFailed.Add(1)
	}

	code := fmt.Sprintf("%d", resp.StatusCode)
	count, _ := metrics.StatusCodes.LoadOrStore(code, new(atomic.Int64))
	count.(*atomic.Int64).Add(1)
}

func printLiveStats(elapsed time.Duration) {
	sent := metrics.RequestsSent.Load()
	success := metrics.RequestsSuccess.Load()
	events := metrics.EventsSent.Load()

	rps := float64(sent) / elapsed.Seconds()
	eps := float64(events) / elapsed.Seconds()

	avgLatency := time.Duration(0)
	if sent > 0 {
		avgLatency = time.Duration(metrics.TotalLatencyNs.Load() / sent)
	}

	successRate := float64(0)
	if sent > 0 {
		successRate = float64(success) / float64(sent) * 100
	}

	fmt.Printf("\r⏱  %s | Req: %d | Events: %d | RPS: %.0f | EPS: %.0f | Avg: %v | Success: %.1f%%",
		elapsed.Round(time.Second),
		sent,
		events,
		rps,
		eps,
		avgLatency.Round(time.Microsecond),
		successRate,
	)
}

func printFinalResults(elapsed time.Duration) {
	sent := metrics.RequestsSent.Load()
	success := metrics.RequestsSuccess.Load()
	failedCount := metrics.RequestsFailed.Load()
	events := metrics.EventsSent.Load()
	bytesSent := metrics.BytesSent.Load()

	rps := float64(sent) / elapsed.Seconds()
	eps := float64(events) / elapsed.Seconds()
	bps := float64(bytesSent) / elapsed.Seconds()

	avgLatency := time.Duration(0)
	minLatency := time.Duration(metrics.MinLatencyNs.Load())
	maxLatency := time.Duration(metrics.MaxLatencyNs.Load())
	if sent > 0 {
		avgLatency = time.Duration(metrics.TotalLatencyNs.Load() / sent)
	}

	successRate := float64(0)
	if sent > 0 {
		successRate = float64(success) / float64(sent) * 100
	}

	fmt.Println("")
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                      LOAD TEST RESULTS                         ║")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Duration:          %-44s ║\n", elapsed.Round(time.Second))
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Total Requests:    %-44d ║\n", sent)
	fmt.Printf("║ Successful:        %-44d ║\n", success)
	fmt.Printf("║ Failed:            %-44d ║\n", failedCount)
	fmt.Printf("║ Success Rate:      %-44.2f ║\n", successRate)
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Total Events:      %-44d ║\n", events)
	fmt.Printf("║ Events/Second:     %-44.2f ║\n", eps)
	fmt.Printf("║ Requests/Second:   %-44.2f ║\n", rps)
	fmt.Printf("║ Throughput:        %-44s ║\n", formatBytes(bps)+"/s")
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Printf("║ Avg Latency:       %-44s ║\n", avgLatency.Round(time.Microsecond))
	fmt.Printf("║ Min Latency:       %-44s ║\n", minLatency.Round(time.Microsecond))
	fmt.Printf("║ Max Latency:       %-44s ║\n", maxLatency.Round(time.Microsecond))
	fmt.Println("╠════════════════════════════════════════════════════════════════╣")
	fmt.Println("║ Status Code Distribution:                                      ║")

	metrics.StatusCodes.Range(func(key, value interface{}) bool {
		code := key.(string)
		count := value.(*atomic.Int64).Load()
		pct := float64(count) / float64(sent) * 100
		fmt.Printf("║   %s: %-8d (%.1f%%)%-36s║\n", code, count, pct, "")
		return true
	})

	if failedCount > 0 {
		fmt.Println("╠════════════════════════════════════════════════════════════════╣")
		fmt.Println("║ Errors:                                                        ║")
		metrics.Errors.Range(func(key, value interface{}) bool {
			errMsg := key.(string)
			count := value.(*atomic.Int64).Load()
			if len(errMsg) > 50 {
				errMsg = errMsg[:50] + "..."
			}
			fmt.Printf("║   %s: %d%-10s║\n", errMsg, count, "")
			return true
		})
	}

	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

func formatBytes(b float64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%.2f B", b)
	}
	div, exp := float64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", b/div, "KMGTPE"[exp])
}
