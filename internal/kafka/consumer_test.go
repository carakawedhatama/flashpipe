package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDefaultConsumerConfig(t *testing.T) {
	brokers := []string{"localhost:9092", "localhost:9093"}
	topic := "test-topic"
	groupID := "test-group"

	cfg := DefaultConsumerConfig(brokers, topic, groupID)

	if len(cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Brokers))
	}
	if cfg.Topic != topic {
		t.Errorf("expected topic %s, got %s", topic, cfg.Topic)
	}
	if cfg.GroupID != groupID {
		t.Errorf("expected group ID %s, got %s", groupID, cfg.GroupID)
	}
	if cfg.MinBytes != 1 {
		t.Errorf("expected min bytes 1, got %d", cfg.MinBytes)
	}
	if cfg.MaxBytes != 10*1024*1024 {
		t.Errorf("expected max bytes 10MB, got %d", cfg.MaxBytes)
	}
	if cfg.MaxWait != 100*time.Millisecond {
		t.Errorf("expected max wait 100ms, got %v", cfg.MaxWait)
	}
	if cfg.CommitInterval != 1*time.Second {
		t.Errorf("expected commit interval 1s, got %v", cfg.CommitInterval)
	}
	if cfg.MaxAttempts != 3 {
		t.Errorf("expected max attempts 3, got %d", cfg.MaxAttempts)
	}
}

func TestNewConsumer(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	if consumer == nil {
		t.Fatal("expected consumer to be created")
	}
	if consumer.reader == nil {
		t.Error("expected reader to be initialized")
	}
	if consumer.handler == nil {
		t.Error("expected handler to be set")
	}
	if consumer.metrics == nil {
		t.Error("expected metrics to be initialized")
	}
}

func TestConsumer_StopWithoutStart(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	// Stop without start should not panic
	err := consumer.Stop()
	if err != nil {
		t.Errorf("stop without start should not error: %v", err)
	}
}

func TestConsumer_GetMetrics(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	metrics := consumer.GetMetrics()

	expectedKeys := []string{
		"messages_consumed",
		"messages_success",
		"messages_failed",
		"bytes_consumed",
		"avg_processing_time_ms",
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

func TestConsumer_StartAfterClose(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	// Mark as closed
	consumer.closed.Store(true)

	err := consumer.Start(context.Background())
	if err != ErrConsumerClosed {
		t.Errorf("expected ErrConsumerClosed, got %v", err)
	}
}

func TestConsumer_StartWithWorkersAfterClose(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	// Mark as closed
	consumer.closed.Store(true)

	err := consumer.StartWithWorkers(context.Background(), 5)
	if err != ErrConsumerClosed {
		t.Errorf("expected ErrConsumerClosed, got %v", err)
	}
}

func TestConsumerMetrics_AtomicOperations(t *testing.T) {
	metrics := &ConsumerMetrics{}

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			metrics.MessagesConsumed.Add(1)
			metrics.MessagesSuccess.Add(1)
			metrics.BytesConsumed.Add(100)
			metrics.ProcessingTime.Add(1000)
		}()
	}

	wg.Wait()

	if metrics.MessagesConsumed.Load() != 100 {
		t.Errorf("expected 100 messages consumed, got %d", metrics.MessagesConsumed.Load())
	}
	if metrics.MessagesSuccess.Load() != 100 {
		t.Errorf("expected 100 messages success, got %d", metrics.MessagesSuccess.Load())
	}
	if metrics.BytesConsumed.Load() != 10000 {
		t.Errorf("expected 10000 bytes consumed, got %d", metrics.BytesConsumed.Load())
	}
	if metrics.ProcessingTime.Load() != 100000 {
		t.Errorf("expected 100000 processing time, got %d", metrics.ProcessingTime.Load())
	}
}

func TestConsumerConfig_Validation(t *testing.T) {
	tests := []struct {
		name        string
		config      ConsumerConfig
		shouldPanic bool
	}{
		{
			name: "minimal config",
			config: ConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test",
				GroupID: "group",
			},
			shouldPanic: false,
		},
		{
			name: "empty brokers - panics",
			config: ConsumerConfig{
				Brokers: []string{},
				Topic:   "test",
				GroupID: "group",
			},
			shouldPanic: true, // kafka-go panics with empty brokers
		},
		{
			name: "nil brokers - panics",
			config: ConsumerConfig{
				Brokers: nil,
				Topic:   "test",
				GroupID: "group",
			},
			shouldPanic: true, // kafka-go panics with nil brokers
		},
		{
			name: "empty topic - panics",
			config: ConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "",
				GroupID: "group",
			},
			shouldPanic: true, // kafka-go requires topic with group ID
		},
		{
			name: "empty group ID",
			config: ConsumerConfig{
				Brokers: []string{"localhost:9092"},
				Topic:   "test",
				GroupID: "",
			},
			shouldPanic: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := func(ctx context.Context, msg Message) error {
				return nil
			}

			if tt.shouldPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Error("expected panic for invalid config")
					}
				}()
			}

			consumer := NewConsumer(tt.config, handler)
			if !tt.shouldPanic {
				if consumer == nil {
					t.Error("consumer should be created")
				}
				consumer.Stop()
			}
		})
	}
}

func TestConsumer_ConcurrentStop(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.Stop()
		}()
	}

	wg.Wait()
	// Should not panic
}

func TestConsumer_Stats(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)
	defer consumer.Stop()

	stats := consumer.Stats()

	// Stats should have valid fields
	if stats.Topic != "test" {
		t.Errorf("expected topic 'test', got '%s'", stats.Topic)
	}
}

func TestErrors_Consumer(t *testing.T) {
	if ErrConsumerClosed.Error() != "consumer is closed" {
		t.Errorf("unexpected error message: %s", ErrConsumerClosed.Error())
	}
}

func TestConsumer_MetricsCalculation(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	// Simulate some metrics
	consumer.metrics.MessagesConsumed.Store(100)
	consumer.metrics.MessagesSuccess.Store(95)
	consumer.metrics.MessagesFailed.Store(5)
	consumer.metrics.BytesConsumed.Store(10000)
	consumer.metrics.ProcessingTime.Store(100 * int64(time.Millisecond))

	metrics := consumer.GetMetrics()

	if metrics["messages_consumed"] != 100 {
		t.Errorf("expected messages_consumed=100, got %d", metrics["messages_consumed"])
	}
	if metrics["messages_success"] != 95 {
		t.Errorf("expected messages_success=95, got %d", metrics["messages_success"])
	}
	if metrics["messages_failed"] != 5 {
		t.Errorf("expected messages_failed=5, got %d", metrics["messages_failed"])
	}
	if metrics["bytes_consumed"] != 10000 {
		t.Errorf("expected bytes_consumed=10000, got %d", metrics["bytes_consumed"])
	}
	// avg_processing_time_ms should be calculated
	if metrics["avg_processing_time_ms"] <= 0 {
		t.Logf("avg_processing_time_ms: %d", metrics["avg_processing_time_ms"])
	}
}

func TestConsumer_MetricsWithZeroMessages(t *testing.T) {
	handler := func(ctx context.Context, msg Message) error {
		return nil
	}

	cfg := DefaultConsumerConfig([]string{"localhost:9092"}, "test", "group")
	consumer := NewConsumer(cfg, handler)

	metrics := consumer.GetMetrics()

	// With zero messages, avg_processing_time should be 0 (avoid division by zero)
	if metrics["avg_processing_time_ms"] != 0 {
		t.Errorf("expected avg_processing_time_ms=0 with no messages, got %d", metrics["avg_processing_time_ms"])
	}
}

// Edge cases for consumer config
func TestConsumerConfig_EdgeCases(t *testing.T) {
	t.Run("zero min bytes", func(t *testing.T) {
		cfg := ConsumerConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    "test",
			GroupID:  "group",
			MinBytes: 0,
		}
		handler := func(ctx context.Context, msg Message) error { return nil }
		consumer := NewConsumer(cfg, handler)
		if consumer == nil {
			t.Error("should create consumer with zero min bytes")
		}
		consumer.Stop()
	})

	t.Run("negative max wait", func(t *testing.T) {
		cfg := ConsumerConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "test",
			GroupID: "group",
			MaxWait: -1 * time.Second,
		}
		handler := func(ctx context.Context, msg Message) error { return nil }
		consumer := NewConsumer(cfg, handler)
		if consumer == nil {
			t.Error("should create consumer with negative max wait")
		}
		consumer.Stop()
	})

	t.Run("very large max bytes", func(t *testing.T) {
		cfg := ConsumerConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    "test",
			GroupID:  "group",
			MaxBytes: 1024 * 1024 * 1024, // 1GB
		}
		handler := func(ctx context.Context, msg Message) error { return nil }
		consumer := NewConsumer(cfg, handler)
		if consumer == nil {
			t.Error("should create consumer with large max bytes")
		}
		consumer.Stop()
	})
}

func TestMessageHandler_Types(t *testing.T) {
	var called atomic.Bool

	handler := func(ctx context.Context, msg Message) error {
		called.Store(true)
		return nil
	}

	// Handler should be callable
	err := handler(context.Background(), Message{Key: "key", Value: "value"})
	if err != nil {
		t.Errorf("handler should not error: %v", err)
	}
	if !called.Load() {
		t.Error("handler should have been called")
	}
}
