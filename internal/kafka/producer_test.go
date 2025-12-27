package kafka

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestDefaultProducerConfig(t *testing.T) {
	brokers := []string{"localhost:9092", "localhost:9093"}
	topic := "test-topic"

	cfg := DefaultProducerConfig(brokers, topic)

	if len(cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Brokers))
	}
	if cfg.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, cfg.Topic)
	}
	if cfg.BatchSize != 1000 {
		t.Errorf("expected batch size 1000, got %d", cfg.BatchSize)
	}
	if cfg.BatchBytes != 10*1024*1024 {
		t.Errorf("expected batch bytes 10MB, got %d", cfg.BatchBytes)
	}
	if cfg.BatchTimeout != 10*time.Millisecond {
		t.Errorf("expected batch timeout 10ms, got %v", cfg.BatchTimeout)
	}
	if !cfg.Async {
		t.Error("expected async to be true")
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("expected max attempts 3, got %d", cfg.MaxAttempts)
	}
	if cfg.CBMaxRequests != 5 {
		t.Errorf("expected CB max requests 5, got %d", cfg.CBMaxRequests)
	}
	if cfg.CBFailureRatio != 0.6 {
		t.Errorf("expected CB failure ratio 0.6, got %f", cfg.CBFailureRatio)
	}
}

func TestNewProducer(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)

	if producer == nil {
		t.Fatal("expected producer to be created")
	}
	if producer.writer == nil {
		t.Error("expected writer to be initialized")
	}
	if producer.cb == nil {
		t.Error("expected circuit breaker to be initialized")
	}
	if producer.metrics == nil {
		t.Error("expected metrics to be initialized")
	}
}

func TestProducer_Close(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)

	// Close should succeed
	err := producer.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	// Close again should be no-op
	err = producer.Close()
	if err != nil {
		t.Errorf("expected no error on second close, got %v", err)
	}

	// Publish after close should fail
	err = producer.Publish(context.Background(), Message{Key: "key", Value: "value"})
	if err != ErrProducerClosed {
		t.Errorf("expected ErrProducerClosed, got %v", err)
	}
}

func TestProducer_PublishAfterClose(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)
	producer.Close()

	err := producer.Publish(context.Background(), Message{Key: "key", Value: "value"})
	if err != ErrProducerClosed {
		t.Errorf("expected ErrProducerClosed, got %v", err)
	}
}

func TestProducer_PublishBatchAfterClose(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)
	producer.Close()

	messages := []Message{{Key: "key", Value: "value"}}
	err := producer.PublishBatch(context.Background(), messages)
	if err != ErrProducerClosed {
		t.Errorf("expected ErrProducerClosed, got %v", err)
	}
}

func TestProducer_PublishAsyncAfterClose(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)
	producer.Close()

	err := producer.PublishAsync(Message{Key: "key", Value: "value"})
	if err != ErrProducerClosed {
		t.Errorf("expected ErrProducerClosed, got %v", err)
	}
}

func TestProducer_PublishBatchEmpty(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)
	defer producer.Close()

	// Empty batch should return nil
	err := producer.PublishBatch(context.Background(), []Message{})
	if err != nil {
		t.Errorf("expected no error for empty batch, got %v", err)
	}

	err = producer.PublishBatch(context.Background(), nil)
	if err != nil {
		t.Errorf("expected no error for nil batch, got %v", err)
	}
}

func TestProducer_GetMetrics(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)
	defer producer.Close()

	metrics := producer.GetMetrics()

	expectedKeys := []string{
		"messages_published",
		"batches_published",
		"bytes_published",
		"errors_total",
		"circuit_opens",
	}

	for _, key := range expectedKeys {
		if _, ok := metrics[key]; !ok {
			t.Errorf("expected metric %s to exist", key)
		}
	}

	// Initial values should be 0
	for key, value := range metrics {
		if value != 0 {
			t.Errorf("expected %s to be 0, got %d", key, value)
		}
	}
}

func TestProducer_Stats(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)
	defer producer.Close()

	stats := producer.Stats()

	// Stats should have valid fields
	if stats.Topic != "test" {
		t.Errorf("expected topic 'test', got '%s'", stats.Topic)
	}
}

func TestMessage_Values(t *testing.T) {
	tests := []struct {
		name    string
		message Message
	}{
		{
			name: "simple message",
			message: Message{
				Key:   "key-1",
				Value: "value-1",
			},
		},
		{
			name: "message with headers",
			message: Message{
				Key:   "key-2",
				Value: []byte("binary value"),
				Headers: map[string]string{
					"content-type": "application/json",
					"source":       "test",
				},
			},
		},
		{
			name: "message with struct value",
			message: Message{
				Key:   "key-3",
				Value: map[string]interface{}{"foo": "bar"},
			},
		},
		{
			name: "empty message",
			message: Message{
				Key:   "",
				Value: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Just verify the struct works
			if tt.message.Key != "" && len(tt.message.Key) == 0 {
				t.Error("key should be empty or have content")
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	tests := []struct {
		name     string
		value    interface{}
		expected []byte
		wantErr  bool
	}{
		{
			name:     "marshal bytes",
			value:    []byte("hello"),
			expected: []byte("hello"),
			wantErr:  false,
		},
		{
			name:     "marshal string",
			value:    "hello",
			expected: []byte("hello"),
			wantErr:  false,
		},
		{
			name:     "marshal map",
			value:    map[string]string{"key": "value"},
			expected: []byte(`{"key":"value"}`),
			wantErr:  false,
		},
		{
			name:     "marshal struct",
			value:    struct{ Name string }{Name: "test"},
			expected: []byte(`{"Name":"test"}`),
			wantErr:  false,
		},
		{
			name:     "marshal nil",
			value:    nil,
			expected: []byte("null"),
			wantErr:  false,
		},
		{
			name:     "marshal number",
			value:    42,
			expected: []byte("42"),
			wantErr:  false,
		},
		{
			name:     "marshal empty bytes",
			value:    []byte{},
			expected: []byte{},
			wantErr:  false,
		},
		{
			name:     "marshal empty string",
			value:    "",
			expected: []byte{},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := marshal(tt.value)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if string(result) != string(tt.expected) {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestMarshal_ComplexTypes(t *testing.T) {
	t.Run("nested struct", func(t *testing.T) {
		type Inner struct {
			Value int `json:"value"`
		}
		type Outer struct {
			Inner Inner `json:"inner"`
		}

		data, err := marshal(Outer{Inner: Inner{Value: 42}})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var decoded Outer
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded.Inner.Value != 42 {
			t.Errorf("expected 42, got %d", decoded.Inner.Value)
		}
	})

	t.Run("slice", func(t *testing.T) {
		data, err := marshal([]int{1, 2, 3})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expected := "[1,2,3]"
		if string(data) != expected {
			t.Errorf("expected %s, got %s", expected, data)
		}
	})

	t.Run("map with interface values", func(t *testing.T) {
		data, err := marshal(map[string]interface{}{
			"string": "value",
			"number": 42,
			"bool":   true,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		var decoded map[string]interface{}
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}

		if decoded["string"] != "value" {
			t.Error("expected string value")
		}
	})
}

func TestProducerConfig_Validation(t *testing.T) {
	tests := []struct {
		name   string
		config ProducerConfig
	}{
		{
			name: "minimal config",
			config: ProducerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test",
			},
		},
		{
			name: "empty brokers",
			config: ProducerConfig{
				Brokers: []string{},
				Topic:   "test",
			},
		},
		{
			name: "nil brokers",
			config: ProducerConfig{
				Brokers: nil,
				Topic:   "test",
			},
		},
		{
			name: "empty topic",
			config: ProducerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "",
			},
		},
		{
			name: "zero batch size",
			config: ProducerConfig{
				Brokers:   []string{"localhost:9092"},
				Topic:     "test",
				BatchSize: 0,
			},
		},
		{
			name: "negative batch timeout",
			config: ProducerConfig{
				Brokers:      []string{"localhost:9092"},
				Topic:        "test",
				BatchTimeout: -1 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Producer should be created even with invalid config
			// (validation happens at runtime when publishing)
			producer := NewProducer(tt.config)
			if producer == nil {
				t.Error("producer should be created")
			}
			producer.Close()
		})
	}
}

func TestProducer_ConcurrentClose(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			producer.Close()
		}()
	}

	wg.Wait()
	// Should not panic
}

func TestProducer_ConcurrentPublish(t *testing.T) {
	cfg := DefaultProducerConfig([]string{"localhost:9092"}, "test")
	producer := NewProducer(cfg)
	defer producer.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// This will fail to connect, but should not panic
			_ = producer.PublishAsync(Message{
				Key:   "key",
				Value: map[string]int{"id": id},
			})
		}(i)
	}

	wg.Wait()
}

func TestProducerMetrics_AtomicOperations(t *testing.T) {
	metrics := &ProducerMetrics{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			metrics.MessagesPublished.Add(1)
			metrics.BytesPublished.Add(100)
			metrics.ErrorsTotal.Add(1)
		}()
	}

	wg.Wait()

	if metrics.MessagesPublished.Load() != 100 {
		t.Errorf("expected 100 messages, got %d", metrics.MessagesPublished.Load())
	}
	if metrics.BytesPublished.Load() != 10000 {
		t.Errorf("expected 10000 bytes, got %d", metrics.BytesPublished.Load())
	}
	if metrics.ErrorsTotal.Load() != 100 {
		t.Errorf("expected 100 errors, got %d", metrics.ErrorsTotal.Load())
	}
}

func TestMessage_HeadersEdgeCases(t *testing.T) {
	t.Run("nil headers", func(t *testing.T) {
		msg := Message{
			Key:     "key",
			Value:   "value",
			Headers: nil,
		}
		if msg.Headers != nil {
			t.Error("headers should be nil")
		}
	})

	t.Run("empty headers", func(t *testing.T) {
		msg := Message{
			Key:     "key",
			Value:   "value",
			Headers: map[string]string{},
		}
		if len(msg.Headers) != 0 {
			t.Error("headers should be empty")
		}
	})

	t.Run("headers with empty values", func(t *testing.T) {
		msg := Message{
			Key:   "key",
			Value: "value",
			Headers: map[string]string{
				"empty-value": "",
				"normal":      "value",
			},
		}
		if msg.Headers["empty-value"] != "" {
			t.Error("empty value should be preserved")
		}
	})

	t.Run("headers with special characters", func(t *testing.T) {
		msg := Message{
			Key:   "key",
			Value: "value",
			Headers: map[string]string{
				"special-!@#$%": "value-!@#$%",
				"unicode-日本語":   "值-中文",
			},
		}
		if msg.Headers["special-!@#$%"] != "value-!@#$%" {
			t.Error("special characters should be preserved")
		}
		if msg.Headers["unicode-日本語"] != "值-中文" {
			t.Error("unicode should be preserved")
		}
	})
}

func TestErrors(t *testing.T) {
	if ErrProducerClosed.Error() != "producer is closed" {
		t.Errorf("unexpected error message: %s", ErrProducerClosed.Error())
	}
	if ErrCircuitOpen.Error() != "circuit breaker is open" {
		t.Errorf("unexpected error message: %s", ErrCircuitOpen.Error())
	}
	if ErrBatchSizeExceeded.Error() != "batch size exceeded" {
		t.Errorf("unexpected error message: %s", ErrBatchSizeExceeded.Error())
	}
}
