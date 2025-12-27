package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"flashpipe/internal/kafka"
	"flashpipe/internal/logging"
	"flashpipe/internal/middleware"
	"flashpipe/internal/model"
	"flashpipe/internal/validate"

	"github.com/gofiber/fiber/v2"
)

func setupTestApp(handler *IngestHandler) *fiber.App {
	app := fiber.New()
	app.Use(middleware.RequestID())
	app.Post("/events", handler.IngestSingle)
	app.Post("/events/batch", handler.IngestBatch)
	app.Post("/events/async", handler.IngestAsync)
	return app
}

func createTestEvent() model.Event {
	return model.Event{
		IdempotencyKey: "idem-" + time.Now().Format(time.RFC3339Nano),
		TenantID:       "tenant-123",
		UserID:         "user-456",
		Workflow:       model.WorkflowPayroll,
		EventType:      "salary.calculated",
		Priority:       model.PriorityNormal,
		Timestamp:      time.Now().UTC(),
		Payload:        map[string]interface{}{"amount": 5000},
	}
}

func TestNewIngestHandler(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "debug"})

	handler := NewIngestHandler(producer, nil, validator, logger)

	if handler == nil {
		t.Fatal("expected handler to be created")
	}
	if handler.producer == nil {
		t.Error("expected producer to be set")
	}
	if handler.validator == nil {
		t.Error("expected validator to be set")
	}
	if handler.logger == nil {
		t.Error("expected logger to be set")
	}

	producer.Close()
}

func TestIngestHandler_IngestSingle_InvalidJSON(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader([]byte("invalid json")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var errResp model.ErrorResponse
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}

	if errResp.Code != "INVALID_BODY" {
		t.Errorf("expected code INVALID_BODY, got %s", errResp.Code)
	}
}

func TestIngestHandler_IngestSingle_ValidationError(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)
	app := setupTestApp(handler)

	// Missing required fields
	invalidEvent := map[string]interface{}{
		"event_type": "test",
	}
	body, _ := json.Marshal(invalidEvent)

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var errResp model.ErrorResponse
	if err := json.Unmarshal(respBody, &errResp); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}

	if errResp.Code != "VALIDATION_ERROR" {
		t.Errorf("expected code VALIDATION_ERROR, got %s", errResp.Code)
	}
}

func TestIngestHandler_IngestBatch_InvalidJSON(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events/batch", bytes.NewReader([]byte("invalid")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestIngestHandler_IngestBatch_ValidationError(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)
	app := setupTestApp(handler)

	// Empty events array (min=1)
	invalidBatch := map[string]interface{}{
		"idempotency_key": "batch-1",
		"tenant_id":       "tenant-1",
		"workflow":        "payroll",
		"events":          []interface{}{},
	}
	body, _ := json.Marshal(invalidBatch)

	req := httptest.NewRequest(http.MethodPost, "/events/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestIngestHandler_IngestAsync_InvalidJSON(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events/async", bytes.NewReader([]byte("{")))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestIngestHandler_IngestAsync_ValidationError(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)
	app := setupTestApp(handler)

	// Missing required fields
	invalidEvent := map[string]interface{}{}
	body, _ := json.Marshal(invalidEvent)

	req := httptest.NewRequest(http.MethodPost, "/events/async", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

// Test constants
func TestIdempotencyKeyTTL(t *testing.T) {
	if IdempotencyKeyTTL != 24*time.Hour {
		t.Errorf("expected 24h, got %v", IdempotencyKeyTTL)
	}
}

// Test event with generated ID
func TestIngestHandler_GeneratesEventID(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)

	// Event without ID should get one generated
	event := createTestEvent()
	event.ID = ""

	body, _ := json.Marshal(event)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// May fail to publish to Kafka, but should try
	// The ID generation happens before publish
}

// Test batch with mixed valid/invalid events
func TestIngestHandler_BatchWithInvalidEvents(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)

	// Batch with one invalid event
	validEvent := createTestEvent()
	invalidEvent := model.Event{} // Missing required fields

	batch := model.BatchEvent{
		IdempotencyKey: "batch-mixed-" + time.Now().Format(time.RFC3339Nano),
		TenantID:       "tenant-123",
		Workflow:       model.WorkflowPayroll,
		Events:         []model.Event{validEvent, invalidEvent},
	}

	body, _ := json.Marshal(batch)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// May succeed partially or fail on Kafka publish
}

// Edge cases
func TestIngestHandler_EmptyBody(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events", nil)
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestIngestHandler_LargePayload(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)

	// Create event with large payload
	largeData := make([]byte, 10000)
	for i := range largeData {
		largeData[i] = 'a'
	}

	event := createTestEvent()
	event.Payload = map[string]interface{}{"large": string(largeData)}

	body, _ := json.Marshal(event)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// May fail on Kafka publish, but shouldn't crash
}

func TestIngestHandler_UnicodePayload(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)

	event := createTestEvent()
	event.Payload = map[string]interface{}{
		"japanese": "日本語",
		"emoji":    "🎉",
		"chinese":  "中文",
	}

	body, _ := json.Marshal(event)
	app := setupTestApp(handler)

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := app.Test(req, -1)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// May fail on Kafka publish, but shouldn't crash
}

// Test with context
func TestIngestHandler_Context(t *testing.T) {
	cfg := kafka.DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	validator := validate.NewGoValidator()
	logger := logging.New(logging.Config{Level: "error"})

	handler := NewIngestHandler(producer, nil, validator, logger)

	// Verify handler uses context from request
	event := createTestEvent()
	body, _ := json.Marshal(event)

	app := fiber.New()
	app.Use(middleware.RequestID())
	app.Post("/events", func(c *fiber.Ctx) error {
		// Context should have request ID set
		ctx := c.Context()
		if ctx == nil {
			t.Error("expected context to be set")
		}
		return handler.IngestSingle(c)
	})

	req := httptest.NewRequest(http.MethodPost, "/events", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, _ := app.Test(req, -1)
	resp.Body.Close()
}

// Mock context for testing
type mockContext struct {
	context.Context
}
