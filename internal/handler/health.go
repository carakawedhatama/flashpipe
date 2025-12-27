package handler

import (
	"context"
	"sync"
	"time"

	"flashpipe/internal/kafka"
	"flashpipe/internal/redis"

	"github.com/gofiber/fiber/v2"
)

// HealthStatus represents the health status of a component.
type HealthStatus struct {
	Status  string `json:"status"`
	Latency string `json:"latency,omitempty"`
	Error   string `json:"error,omitempty"`
}

// HealthResponse represents the overall health response.
type HealthResponse struct {
	Status     string                  `json:"status"`
	Version    string                  `json:"version"`
	Timestamp  int64                   `json:"timestamp"`
	Components map[string]HealthStatus `json:"components"`
}

// HealthHandler handles health check requests.
type HealthHandler struct {
	version  string
	redis    *redis.Client
	producer *kafka.Producer
}

// NewHealthHandler creates a new health handler.
func NewHealthHandler(version string, redisClient *redis.Client, producer *kafka.Producer) *HealthHandler {
	return &HealthHandler{
		version:  version,
		redis:    redisClient,
		producer: producer,
	}
}

// Liveness returns a simple liveness probe response.
// GET /health/live
func (h *HealthHandler) Liveness(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "ok",
		"timestamp": time.Now().UnixMilli(),
	})
}

// Readiness checks if the service is ready to accept traffic.
// GET /health/ready
func (h *HealthHandler) Readiness(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Second)
	defer cancel()

	components := make(map[string]HealthStatus)
	overallStatus := "healthy"
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Check Redis
	if h.redis != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			err := h.redis.Ping(ctx)
			latency := time.Since(start)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				components["redis"] = HealthStatus{
					Status:  "unhealthy",
					Latency: latency.String(),
					Error:   err.Error(),
				}
				overallStatus = "degraded"
			} else {
				components["redis"] = HealthStatus{
					Status:  "healthy",
					Latency: latency.String(),
				}
			}
		}()
	}

	// Check Kafka producer stats
	if h.producer != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats := h.producer.Stats()

			mu.Lock()
			defer mu.Unlock()
			if stats.Errors > 0 && float64(stats.Errors)/float64(stats.Messages) > 0.1 {
				components["kafka"] = HealthStatus{
					Status: "degraded",
				}
				if overallStatus == "healthy" {
					overallStatus = "degraded"
				}
			} else {
				components["kafka"] = HealthStatus{
					Status: "healthy",
				}
			}
		}()
	}

	wg.Wait()

	response := HealthResponse{
		Status:     overallStatus,
		Version:    h.version,
		Timestamp:  time.Now().UnixMilli(),
		Components: components,
	}

	status := fiber.StatusOK
	if overallStatus != "healthy" {
		status = fiber.StatusServiceUnavailable
	}

	return c.Status(status).JSON(response)
}

// Metrics returns internal metrics.
// GET /health/metrics
func (h *HealthHandler) Metrics(c *fiber.Ctx) error {
	metrics := make(map[string]interface{})

	if h.producer != nil {
		metrics["producer"] = h.producer.GetMetrics()
		stats := h.producer.Stats()
		metrics["kafka_stats"] = map[string]interface{}{
			"messages":     stats.Messages,
			"bytes":        stats.Bytes,
			"errors":       stats.Errors,
			"max_attempts": stats.MaxAttempts,
			"write_time":   stats.WriteTime.Avg.String(),
			"wait_time":    stats.WaitTime.Avg.String(),
			"retries":      stats.Retries,
		}
	}

	if h.redis != nil {
		poolStats := h.redis.Stats()
		metrics["redis_pool"] = map[string]interface{}{
			"hits":        poolStats.Hits,
			"misses":      poolStats.Misses,
			"timeouts":    poolStats.Timeouts,
			"total_conns": poolStats.TotalConns,
			"idle_conns":  poolStats.IdleConns,
			"stale_conns": poolStats.StaleConns,
		}
	}

	return c.JSON(metrics)
}
