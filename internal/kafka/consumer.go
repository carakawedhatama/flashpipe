package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

var (
	ErrConsumerClosed = errors.New("consumer is closed")
)

// ConsumerConfig holds configuration for the Kafka consumer.
type ConsumerConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	MinBytes       int
	MaxBytes       int
	MaxWait        time.Duration
	CommitInterval time.Duration
	StartOffset    int64
	MaxAttempts    int
}

// DefaultConsumerConfig returns sensible defaults.
func DefaultConsumerConfig(brokers []string, topic, groupID string) ConsumerConfig {
	return ConsumerConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024, // 10MB
		MaxWait:        100 * time.Millisecond,
		CommitInterval: 1 * time.Second,
		StartOffset:    kafka.LastOffset,
		MaxAttempts:    3,
	}
}

// MessageHandler processes a consumed message.
type MessageHandler func(ctx context.Context, msg Message) error

// Consumer is a Kafka consumer with automatic offset management.
type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
	closed  atomic.Bool
	wg      sync.WaitGroup
	metrics *ConsumerMetrics
}

// ConsumerMetrics tracks consumer performance.
type ConsumerMetrics struct {
	MessagesConsumed atomic.Int64
	MessagesSuccess  atomic.Int64
	MessagesFailed   atomic.Int64
	BytesConsumed    atomic.Int64
	ProcessingTime   atomic.Int64 // nanoseconds
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(cfg ConsumerConfig, handler MessageHandler) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.Topic,
		GroupID:        cfg.GroupID,
		MinBytes:       cfg.MinBytes,
		MaxBytes:       cfg.MaxBytes,
		MaxWait:        cfg.MaxWait,
		CommitInterval: cfg.CommitInterval,
		StartOffset:    cfg.StartOffset,
		MaxAttempts:    cfg.MaxAttempts,
	})

	return &Consumer{
		reader:  reader,
		handler: handler,
		metrics: &ConsumerMetrics{},
	}
}

// Start starts consuming messages.
func (c *Consumer) Start(ctx context.Context) error {
	if c.closed.Load() {
		return ErrConsumerClosed
	}

	c.wg.Add(1)
	go c.consume(ctx)

	return nil
}

// consume is the main consumption loop.
func (c *Consumer) consume(ctx context.Context) {
	defer c.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if c.closed.Load() {
			return
		}

		// Fetch message
		kafkaMsg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			continue
		}

		c.metrics.MessagesConsumed.Add(1)
		c.metrics.BytesConsumed.Add(int64(len(kafkaMsg.Value)))

		// Convert to our message type
		msg := Message{
			Key:     string(kafkaMsg.Key),
			Value:   kafkaMsg.Value,
			Headers: make(map[string]string),
		}

		for _, h := range kafkaMsg.Headers {
			msg.Headers[h.Key] = string(h.Value)
		}

		// Process message
		start := time.Now()
		if err := c.handler(ctx, msg); err != nil {
			c.metrics.MessagesFailed.Add(1)
			// In production, implement retry logic or dead-letter queue here
			continue
		}

		c.metrics.MessagesSuccess.Add(1)
		c.metrics.ProcessingTime.Add(time.Since(start).Nanoseconds())

		// Commit offset
		if err := c.reader.CommitMessages(ctx, kafkaMsg); err != nil {
			// Log error but continue
			continue
		}
	}
}

// StartWithWorkers starts multiple consumer workers.
func (c *Consumer) StartWithWorkers(ctx context.Context, workers int) error {
	if c.closed.Load() {
		return ErrConsumerClosed
	}

	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		go c.consume(ctx)
	}

	return nil
}

// Stop stops the consumer gracefully.
func (c *Consumer) Stop() error {
	if c.closed.Swap(true) {
		return nil
	}

	c.wg.Wait()
	return c.reader.Close()
}

// Stats returns reader statistics.
func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

// GetMetrics returns current metrics values.
func (c *Consumer) GetMetrics() map[string]int64 {
	processed := c.metrics.MessagesConsumed.Load()
	var avgProcessingTime int64
	if processed > 0 {
		avgProcessingTime = c.metrics.ProcessingTime.Load() / processed / int64(time.Millisecond)
	}

	return map[string]int64{
		"messages_consumed":      processed,
		"messages_success":       c.metrics.MessagesSuccess.Load(),
		"messages_failed":        c.metrics.MessagesFailed.Load(),
		"bytes_consumed":         c.metrics.BytesConsumed.Load(),
		"avg_processing_time_ms": avgProcessingTime,
	}
}
