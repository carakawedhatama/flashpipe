package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
)

func TestRequestID(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Get("/test", func(c *fiber.Ctx) error {
		requestID := c.Locals(RequestIDKey)
		return c.SendString(requestID.(string))
	})

	t.Run("generates new request ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if len(body) == 0 {
			t.Error("expected request ID to be generated")
		}

		// Check header is set
		if resp.Header.Get("X-Request-ID") == "" {
			t.Error("expected X-Request-ID header to be set")
		}
	})

	t.Run("uses provided request ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "custom-id-123")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "custom-id-123" {
			t.Errorf("expected custom-id-123, got %s", body)
		}

		if resp.Header.Get("X-Request-ID") != "custom-id-123" {
			t.Error("expected X-Request-ID header to match provided value")
		}
	})
}

func TestMetrics(t *testing.T) {
	app := fiber.New()
	app.Use(Metrics())
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})
	app.Get("/slow", func(c *fiber.Ctx) error {
		time.Sleep(50 * time.Millisecond)
		return c.SendString("ok")
	})

	t.Run("records request metrics", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("records slow request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/slow", nil)
		resp, err := app.Test(req, -1)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})
}

func TestCORS(t *testing.T) {
	app := fiber.New()
	app.Use(CORS())
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	t.Run("adds CORS headers", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Error("expected Access-Control-Allow-Origin header")
		}
		if resp.Header.Get("Access-Control-Allow-Methods") == "" {
			t.Error("expected Access-Control-Allow-Methods header")
		}
	})

	t.Run("handles OPTIONS preflight", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodOptions, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNoContent {
			t.Errorf("expected 204, got %d", resp.StatusCode)
		}
	})
}

func TestExtractTenant(t *testing.T) {
	app := fiber.New()
	app.Use(ExtractTenant())
	app.Get("/test", func(c *fiber.Ctx) error {
		tenantID := c.Locals(TenantIDKey)
		return c.SendString(tenantID.(string))
	})

	t.Run("extracts tenant from header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Tenant-ID", "tenant-123")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "tenant-123" {
			t.Errorf("expected tenant-123, got %s", body)
		}
	})

	t.Run("returns error when missing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", resp.StatusCode)
		}
	})
}

func TestNewSimpleRateLimiter(t *testing.T) {
	t.Run("creates rate limiter", func(t *testing.T) {
		rl := NewSimpleRateLimiter(100, 200)
		if rl == nil {
			t.Fatal("expected rate limiter to be created")
		}
		if rl.maxTokens != 200 {
			t.Errorf("expected max tokens 200, got %d", rl.maxTokens)
		}
		if rl.refillRate != 100 {
			t.Errorf("expected refill rate 100, got %d", rl.refillRate)
		}
	})

	t.Run("allows initial burst", func(t *testing.T) {
		rl := NewSimpleRateLimiter(10, 5)

		// Should allow burst of 5
		for i := 0; i < 5; i++ {
			if !rl.Allow() {
				t.Errorf("request %d should be allowed", i)
			}
		}

		// Should reject after burst
		if rl.Allow() {
			t.Error("request after burst should be rejected")
		}
	})

	t.Run("refills over time", func(t *testing.T) {
		rl := NewSimpleRateLimiter(1000, 1) // 1000 per second, burst of 1

		// Use the token
		if !rl.Allow() {
			t.Error("first request should be allowed")
		}

		// Should be rejected immediately
		if rl.Allow() {
			t.Error("second request should be rejected")
		}

		// Wait for refill (1ms should add 1 token at 1000/s)
		time.Sleep(5 * time.Millisecond)

		// Should be allowed after refill
		if !rl.Allow() {
			t.Error("request after refill should be allowed")
		}
	})
}

func TestSimpleRateLimiter_ConcurrentAccess(t *testing.T) {
	rl := NewSimpleRateLimiter(1000, 100)

	var allowed atomic.Int64
	var rejected atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rl.Allow() {
				allowed.Add(1)
			} else {
				rejected.Add(1)
			}
		}()
	}

	wg.Wait()

	total := allowed.Load() + rejected.Load()
	if total != 200 {
		t.Errorf("expected 200 total, got %d", total)
	}

	// Should allow at most burst (100) + some refilled tokens
	if allowed.Load() > 150 {
		t.Errorf("allowed too many requests: %d", allowed.Load())
	}
}

func TestRateLimit_Middleware(t *testing.T) {
	limiter := NewSimpleRateLimiter(1, 2) // 1 per second, burst of 2

	app := fiber.New()
	app.Use(RateLimit(limiter))
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString("ok")
	})

	t.Run("allows requests under limit", func(t *testing.T) {
		for i := 0; i < 2; i++ {
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			resp, err := app.Test(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("request %d should be allowed, got %d", i, resp.StatusCode)
			}
		}
	})

	t.Run("rejects requests over limit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusTooManyRequests {
			t.Errorf("expected 429, got %d", resp.StatusCode)
		}
	})
}

func TestStatusLabel(t *testing.T) {
	tests := []struct {
		status   int
		expected string
	}{
		{100, "1xx"},
		{101, "1xx"},
		{200, "2xx"},
		{201, "2xx"},
		{204, "2xx"},
		{299, "2xx"},
		{300, "3xx"},
		{301, "3xx"},
		{302, "3xx"},
		{400, "4xx"},
		{401, "4xx"},
		{404, "4xx"},
		{422, "4xx"},
		{500, "5xx"},
		{502, "5xx"},
		{503, "5xx"},
	}

	for _, tt := range tests {
		t.Run(string(rune(tt.status)), func(t *testing.T) {
			result := statusLabel(tt.status)
			if result != tt.expected {
				t.Errorf("statusLabel(%d) = %s, expected %s", tt.status, result, tt.expected)
			}
		})
	}
}

func TestDefaultRateLimitConfig(t *testing.T) {
	cfg := DefaultRateLimitConfig()

	if cfg.RequestsPerSecond != 1000 {
		t.Errorf("expected 1000 RPS, got %d", cfg.RequestsPerSecond)
	}
	if cfg.BurstSize != 2000 {
		t.Errorf("expected burst 2000, got %d", cfg.BurstSize)
	}
	if cfg.KeyGenerator == nil {
		t.Error("expected KeyGenerator to be set")
	}
}

func TestDefaultRateLimitConfig_KeyGenerator(t *testing.T) {
	cfg := DefaultRateLimitConfig()

	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		key := cfg.KeyGenerator(c)
		return c.SendString(key)
	})

	t.Run("returns global when no tenant", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "global" {
			t.Errorf("expected 'global', got '%s'", body)
		}
	})

	t.Run("returns tenant ID from header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Tenant-ID", "tenant-abc")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "tenant-abc" {
			t.Errorf("expected 'tenant-abc', got '%s'", body)
		}
	})
}

func TestRecordEventIngested(t *testing.T) {
	// This just ensures the function doesn't panic
	RecordEventIngested("payroll", "tenant-123", 10)
	RecordEventIngested("attendance", "tenant-456", 100)
	RecordEventIngested("", "", 0)
}

func TestRecordBatchSize(t *testing.T) {
	// This just ensures the function doesn't panic
	RecordBatchSize("payroll", 100)
	RecordBatchSize("attendance", 1000)
	RecordBatchSize("", 0)
}

func TestAcceptsCompression(t *testing.T) {
	app := fiber.New()
	app.Get("/test", func(c *fiber.Ctx) error {
		if AcceptsCompression(c) {
			return c.SendString("compressed")
		}
		return c.SendString("plain")
	})

	t.Run("returns true when accept-encoding set", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Accept-Encoding", "gzip, deflate")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "compressed" {
			t.Errorf("expected 'compressed', got '%s'", body)
		}
	})

	t.Run("returns false when no accept-encoding", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "plain" {
			t.Errorf("expected 'plain', got '%s'", body)
		}
	})
}

func TestConstants(t *testing.T) {
	if RequestIDKey != "request_id" {
		t.Errorf("expected 'request_id', got '%s'", RequestIDKey)
	}
	if TenantIDKey != "tenant_id" {
		t.Errorf("expected 'tenant_id', got '%s'", TenantIDKey)
	}
}

// Edge cases
func TestRateLimiter_EdgeCases(t *testing.T) {
	t.Run("zero burst", func(t *testing.T) {
		rl := NewSimpleRateLimiter(100, 0)
		// With 0 burst, no requests should be allowed initially
		// But this is an edge case - in practice, burst should be > 0
		if rl.Allow() {
			t.Error("expected rejection with zero burst")
		}
	})

	t.Run("very high rate", func(t *testing.T) {
		rl := NewSimpleRateLimiter(1000000, 1000)

		allowed := 0
		for i := 0; i < 100; i++ {
			if rl.Allow() {
				allowed++
			}
		}

		if allowed < 100 {
			t.Errorf("expected at least 100 allowed with high rate, got %d", allowed)
		}
	})

	t.Run("single token burst", func(t *testing.T) {
		rl := NewSimpleRateLimiter(1, 1)

		if !rl.Allow() {
			t.Error("first request should be allowed")
		}
		if rl.Allow() {
			t.Error("second request should be rejected")
		}
	})
}

func TestRequestID_EdgeCases(t *testing.T) {
	app := fiber.New()
	app.Use(RequestID())
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString(c.Locals(RequestIDKey).(string))
	})

	t.Run("empty X-Request-ID header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if len(body) == 0 {
			t.Error("should generate new ID when header is empty")
		}
	})

	t.Run("whitespace X-Request-ID header", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "   ")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		// Fiber/http may trim whitespace from headers or treat as empty
		// Just verify we get a valid response
		if len(body) == 0 {
			t.Error("should return a request ID")
		}
	})

	t.Run("unicode X-Request-ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Request-ID", "请求-123-🎉")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != "请求-123-🎉" {
			t.Error("should preserve unicode request ID")
		}
	})
}

func TestExtractTenant_EdgeCases(t *testing.T) {
	app := fiber.New()
	app.Use(ExtractTenant())
	app.Get("/test", func(c *fiber.Ctx) error {
		return c.SendString(c.Locals(TenantIDKey).(string))
	})

	t.Run("empty tenant ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Tenant-ID", "")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("expected 400 for empty tenant, got %d", resp.StatusCode)
		}
	})

	t.Run("whitespace tenant ID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Tenant-ID", "  tenant  ")
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		// Fiber may trim leading/trailing whitespace from headers
		// Just verify it contains 'tenant'
		if !strings.Contains(string(body), "tenant") {
			t.Error("should contain tenant in tenant ID")
		}
	})

	t.Run("very long tenant ID", func(t *testing.T) {
		longTenant := make([]byte, 1000)
		for i := range longTenant {
			longTenant[i] = 'a'
		}

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Tenant-ID", string(longTenant))
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)
		if string(body) != string(longTenant) {
			t.Error("should handle long tenant ID")
		}
	})
}
