package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"flashpipe/internal/kafka"

	"github.com/gofiber/fiber/v2"
)

func setupHealthApp(handler *HealthHandler) *fiber.App {
	app := fiber.New()
	app.Get("/health/live", handler.Liveness)
	app.Get("/health/ready", handler.Readiness)
	app.Get("/health/metrics", handler.Metrics)
	return app
}

func TestNewHealthHandler(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	handler := NewHealthHandler("1.0.0", nil, producer)

	if handler == nil {
		t.Fatal("expected handler to be created")
	}
	if handler.version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", handler.version)
	}
	if handler.producer == nil {
		t.Error("expected producer to be set")
	}
}

func TestHealthHandler_Liveness(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	handler := NewHealthHandler("1.0.0", nil, producer)
	app := setupHealthApp(handler)

	req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result["status"] != "ok" {
		t.Errorf("expected status 'ok', got '%v'", result["status"])
	}
	if result["timestamp"] == nil {
		t.Error("expected timestamp to be set")
	}
}

func TestHealthHandler_Readiness_NoRedis(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	handler := NewHealthHandler("1.0.0", nil, producer)
	app := setupHealthApp(handler)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Without Redis, should still return (may be degraded)
	body, _ := io.ReadAll(resp.Body)
	var result HealthResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result.Version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", result.Version)
	}
	if result.Timestamp == 0 {
		t.Error("expected timestamp to be set")
	}
}

func TestHealthHandler_Metrics(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	handler := NewHealthHandler("1.0.0", nil, producer)
	app := setupHealthApp(handler)

	req := httptest.NewRequest(http.MethodGet, "/health/metrics", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	// Should have producer metrics
	if result["producer"] == nil {
		t.Error("expected producer metrics")
	}
	if result["kafka_stats"] == nil {
		t.Error("expected kafka stats")
	}
}

func TestHealthHandler_MetricsWithoutProducer(t *testing.T) {
	handler := NewHealthHandler("1.0.0", nil, nil)
	app := setupHealthApp(handler)

	req := httptest.NewRequest(http.MethodGet, "/health/metrics", nil)
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHealthResponse_Fields(t *testing.T) {
	resp := HealthResponse{
		Status:    "healthy",
		Version:   "1.0.0",
		Timestamp: 1234567890,
		Components: map[string]HealthStatus{
			"redis": {Status: "healthy", Latency: "1ms"},
			"kafka": {Status: "healthy"},
		},
	}

	if resp.Status != "healthy" {
		t.Errorf("expected status healthy, got %s", resp.Status)
	}
	if resp.Version != "1.0.0" {
		t.Errorf("expected version 1.0.0, got %s", resp.Version)
	}
	if len(resp.Components) != 2 {
		t.Errorf("expected 2 components, got %d", len(resp.Components))
	}
}

func TestHealthStatus_Fields(t *testing.T) {
	status := HealthStatus{
		Status:  "unhealthy",
		Latency: "100ms",
		Error:   "connection refused",
	}

	if status.Status != "unhealthy" {
		t.Errorf("expected status unhealthy, got %s", status.Status)
	}
	if status.Latency != "100ms" {
		t.Errorf("expected latency 100ms, got %s", status.Latency)
	}
	if status.Error != "connection refused" {
		t.Errorf("expected error 'connection refused', got %s", status.Error)
	}
}

// Edge cases
func TestHealthHandler_Liveness_MultipleRequests(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	handler := NewHealthHandler("1.0.0", nil, producer)
	app := setupHealthApp(handler)

	// Multiple rapid requests should all succeed
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("request %d: expected 200, got %d", i, resp.StatusCode)
		}
	}
}

func TestHealthHandler_EmptyVersion(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	handler := NewHealthHandler("", nil, producer)
	app := setupHealthApp(handler)

	req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result HealthResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}

	if result.Version != "" {
		t.Errorf("expected empty version, got %s", result.Version)
	}
}

func TestHealthHandler_NilProducer(t *testing.T) {
	handler := NewHealthHandler("1.0.0", nil, nil)
	app := setupHealthApp(handler)

	t.Run("liveness without producer", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health/live", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})

	t.Run("readiness without producer", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/health/ready", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()

		// Should still return OK even without producer
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected 200, got %d", resp.StatusCode)
		}
	})
}

// JSON serialization tests
func TestHealthResponse_JSONSerialization(t *testing.T) {
	resp := HealthResponse{
		Status:    "healthy",
		Version:   "1.0.0",
		Timestamp: 1234567890,
		Components: map[string]HealthStatus{
			"redis": {Status: "healthy", Latency: "1ms"},
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded HealthResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.Status != resp.Status {
		t.Errorf("expected status %s, got %s", resp.Status, decoded.Status)
	}
	if decoded.Version != resp.Version {
		t.Errorf("expected version %s, got %s", resp.Version, decoded.Version)
	}
	if decoded.Timestamp != resp.Timestamp {
		t.Errorf("expected timestamp %d, got %d", resp.Timestamp, decoded.Timestamp)
	}
}

func TestHealthStatus_JSONSerialization(t *testing.T) {
	tests := []struct {
		name   string
		status HealthStatus
	}{
		{
			name: "healthy status",
			status: HealthStatus{
				Status:  "healthy",
				Latency: "1ms",
			},
		},
		{
			name: "unhealthy status with error",
			status: HealthStatus{
				Status: "unhealthy",
				Error:  "connection failed",
			},
		},
		{
			name:   "empty status",
			status: HealthStatus{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.status)
			if err != nil {
				t.Fatalf("failed to marshal: %v", err)
			}

			var decoded HealthStatus
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("failed to unmarshal: %v", err)
			}

			if decoded.Status != tt.status.Status {
				t.Errorf("expected status %s, got %s", tt.status.Status, decoded.Status)
			}
		})
	}
}
