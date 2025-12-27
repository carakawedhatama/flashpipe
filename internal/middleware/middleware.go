package middleware

import (
	"sync/atomic"
	"time"

	"flashpipe/internal/logging"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// RequestIDKey is the context key for request ID.
const RequestIDKey = "request_id"

// --- Request ID Middleware ---

// RequestID adds a unique request ID to each request.
func RequestID() fiber.Handler {
	return func(c *fiber.Ctx) error {
		requestID := c.Get("X-Request-ID")
		if requestID == "" {
			requestID = uuid.New().String()
		}
		c.Locals(RequestIDKey, requestID)
		c.Set("X-Request-ID", requestID)
		return c.Next()
	}
}

// --- Recovery Middleware ---

// Recovery recovers from panics and logs them.
func Recovery(logger *logging.Logger) fiber.Handler {
	return func(c *fiber.Ctx) error {
		defer func() {
			if r := recover(); r != nil {
				requestID, _ := c.Locals(RequestIDKey).(string)
				logger.WithRequestID(requestID).Error(
					"panic recovered",
					nil,
					logging.F("panic", r),
					logging.F("path", c.Path()),
					logging.F("method", c.Method()),
				)
				c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"code":    "INTERNAL_ERROR",
					"message": "An unexpected error occurred",
				})
			}
		}()
		return c.Next()
	}
}

// --- Metrics Middleware ---

var (
	httpRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flashpipe_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flashpipe_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		},
		[]string{"method", "path"},
	)

	httpRequestsInFlight = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "flashpipe_http_requests_in_flight",
			Help: "Number of HTTP requests currently being processed",
		},
	)

	eventsIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "flashpipe_events_ingested_total",
			Help: "Total number of events ingested",
		},
		[]string{"workflow", "tenant_id"},
	)

	eventsBatchSize = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "flashpipe_events_batch_size",
			Help:    "Size of event batches",
			Buckets: []float64{1, 10, 50, 100, 500, 1000},
		},
		[]string{"workflow"},
	)
)

// Metrics collects HTTP metrics.
func Metrics() fiber.Handler {
	return func(c *fiber.Ctx) error {
		start := time.Now()
		httpRequestsInFlight.Inc()
		defer httpRequestsInFlight.Dec()

		err := c.Next()

		duration := time.Since(start).Seconds()
		status := c.Response().StatusCode()
		method := c.Method()
		path := c.Route().Path

		httpRequestsTotal.WithLabelValues(method, path, statusLabel(status)).Inc()
		httpRequestDuration.WithLabelValues(method, path).Observe(duration)

		return err
	}
}

func statusLabel(status int) string {
	switch {
	case status >= 500:
		return "5xx"
	case status >= 400:
		return "4xx"
	case status >= 300:
		return "3xx"
	case status >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

// RecordEventIngested records an event ingestion metric.
func RecordEventIngested(workflow, tenantID string, count int) {
	eventsIngestedTotal.WithLabelValues(workflow, tenantID).Add(float64(count))
}

// RecordBatchSize records a batch size metric.
func RecordBatchSize(workflow string, size int) {
	eventsBatchSize.WithLabelValues(workflow).Observe(float64(size))
}

// --- Rate Limiting Middleware ---

// RateLimitConfig holds rate limiter configuration.
type RateLimitConfig struct {
	RequestsPerSecond int
	BurstSize         int
	KeyGenerator      func(*fiber.Ctx) string
}

// DefaultRateLimitConfig returns default rate limit settings.
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		RequestsPerSecond: 1000,
		BurstSize:         2000,
		KeyGenerator: func(c *fiber.Ctx) string {
			// Default: rate limit by tenant ID from header
			return c.Get("X-Tenant-ID", "global")
		},
	}
}

// SimpleRateLimiter implements a simple in-memory rate limiter.
// For production, use Redis-based rate limiting.
type SimpleRateLimiter struct {
	tokens     atomic.Int64
	maxTokens  int64
	refillRate int64
	lastRefill atomic.Int64
}

// NewSimpleRateLimiter creates a simple rate limiter.
func NewSimpleRateLimiter(rps, burst int) *SimpleRateLimiter {
	rl := &SimpleRateLimiter{
		maxTokens:  int64(burst),
		refillRate: int64(rps),
	}
	rl.tokens.Store(int64(burst))
	rl.lastRefill.Store(time.Now().UnixNano())
	return rl
}

// Allow checks if a request is allowed.
func (rl *SimpleRateLimiter) Allow() bool {
	now := time.Now().UnixNano()
	last := rl.lastRefill.Load()
	elapsed := now - last

	// Refill tokens based on elapsed time
	if elapsed > 0 {
		tokensToAdd := (elapsed * rl.refillRate) / int64(time.Second)
		if tokensToAdd > 0 {
			if rl.lastRefill.CompareAndSwap(last, now) {
				current := rl.tokens.Load()
				newTokens := current + tokensToAdd
				if newTokens > rl.maxTokens {
					newTokens = rl.maxTokens
				}
				rl.tokens.Store(newTokens)
			}
		}
	}

	// Try to consume a token
	for {
		current := rl.tokens.Load()
		if current <= 0 {
			return false
		}
		if rl.tokens.CompareAndSwap(current, current-1) {
			return true
		}
	}
}

// RateLimit applies rate limiting middleware.
func RateLimit(limiter *SimpleRateLimiter) fiber.Handler {
	return func(c *fiber.Ctx) error {
		if !limiter.Allow() {
			return c.Status(fiber.StatusTooManyRequests).JSON(fiber.Map{
				"code":    "RATE_LIMITED",
				"message": "Too many requests",
			})
		}
		return c.Next()
	}
}

// --- Tenant Extraction Middleware ---

const TenantIDKey = "tenant_id"

// ExtractTenant extracts tenant ID from header.
func ExtractTenant() fiber.Handler {
	return func(c *fiber.Ctx) error {
		tenantID := c.Get("X-Tenant-ID")
		if tenantID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"code":    "MISSING_TENANT",
				"message": "X-Tenant-ID header is required",
			})
		}
		c.Locals(TenantIDKey, tenantID)
		return c.Next()
	}
}

// --- CORS Middleware ---

// CORS adds CORS headers.
func CORS() fiber.Handler {
	return func(c *fiber.Ctx) error {
		c.Set("Access-Control-Allow-Origin", "*")
		c.Set("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
		c.Set("Access-Control-Allow-Headers", "Content-Type,Authorization,X-Request-ID,X-Tenant-ID,X-Idempotency-Key")
		c.Set("Access-Control-Max-Age", "86400")

		if c.Method() == fiber.MethodOptions {
			return c.SendStatus(fiber.StatusNoContent)
		}

		return c.Next()
	}
}

// --- Compression detection ---

// AcceptsCompression checks if client accepts compression.
func AcceptsCompression(c *fiber.Ctx) bool {
	accept := c.Get("Accept-Encoding")
	return len(accept) > 0
}
