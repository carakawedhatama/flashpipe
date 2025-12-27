package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker"
)

var (
	ErrProducerClosed    = errors.New("producer is closed")
	ErrCircuitOpen       = errors.New("circuit breaker is open")
	ErrBatchSizeExceeded = errors.New("batch size exceeded")
)

// ProducerConfig holds configuration for the Kafka producer.
type ProducerConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	BatchBytes   int64
	BatchTimeout time.Duration
	RequiredAcks kafka.RequiredAcks
	Async        bool
	Compression  kafka.Compression
	MaxAttempts  int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	Balancer     kafka.Balancer
	// Circuit breaker settings
	CBMaxRequests  uint32
	CBInterval     time.Duration
	CBTimeout      time.Duration
	CBFailureRatio float64
}

// DefaultProducerConfig returns sensible defaults for high-throughput.
func DefaultProducerConfig(brokers []string, topic string) ProducerConfig {
	return ProducerConfig{
		Brokers:        brokers,
		Topic:          topic,
		BatchSize:      1000,
		BatchBytes:     10 * 1024 * 1024, // 10MB
		BatchTimeout:   10 * time.Millisecond,
		RequiredAcks:   kafka.RequireOne,
		Async:          true,
		Compression:    kafka.Lz4,
		MaxAttempts:    3,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		Balancer:       &kafka.Hash{},
		CBMaxRequests:  5,
		CBInterval:     10 * time.Second,
		CBTimeout:      30 * time.Second,
		CBFailureRatio: 0.6,
	}
}

// Producer is a high-throughput Kafka producer with circuit breaker and batching.
type Producer struct {
	writer  *kafka.Writer
	cb      *gobreaker.CircuitBreaker
	closed  atomic.Bool
	metrics *ProducerMetrics
	mu      sync.RWMutex
}

// ProducerMetrics tracks producer performance.
type ProducerMetrics struct {
	MessagesPublished atomic.Int64
	BatchesPublished  atomic.Int64
	BytesPublished    atomic.Int64
	ErrorsTotal       atomic.Int64
	CircuitOpens      atomic.Int64
}

// NewProducer creates a new high-throughput Kafka producer.
func NewProducer(cfg ProducerConfig) *Producer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Topic:                  cfg.Topic,
		Balancer:               cfg.Balancer,
		RequiredAcks:           cfg.RequiredAcks,
		BatchSize:              cfg.BatchSize,
		BatchBytes:             cfg.BatchBytes,
		BatchTimeout:           cfg.BatchTimeout,
		Async:                  cfg.Async,
		Compression:            cfg.Compression,
		MaxAttempts:            cfg.MaxAttempts,
		ReadTimeout:            cfg.ReadTimeout,
		WriteTimeout:           cfg.WriteTimeout,
		AllowAutoTopicCreation: true,
	}

	metrics := &ProducerMetrics{}

	cbSettings := gobreaker.Settings{
		Name:        fmt.Sprintf("kafka-producer-%s", cfg.Topic),
		MaxRequests: cfg.CBMaxRequests,
		Interval:    cfg.CBInterval,
		Timeout:     cfg.CBTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= cfg.CBFailureRatio
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			if to == gobreaker.StateOpen {
				metrics.CircuitOpens.Add(1)
			}
		},
	}

	return &Producer{
		writer:  writer,
		cb:      gobreaker.NewCircuitBreaker(cbSettings),
		metrics: metrics,
	}
}

// Message represents a message to be published.
type Message struct {
	Key     string
	Value   interface{}
	Headers map[string]string
}

// Publish publishes a single message to Kafka.
func (p *Producer) Publish(ctx context.Context, msg Message) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}

	value, err := marshal(msg.Value)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(msg.Key),
		Value: value,
	}

	if len(msg.Headers) > 0 {
		kafkaMsg.Headers = make([]kafka.Header, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			kafkaMsg.Headers = append(kafkaMsg.Headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	_, err = p.cb.Execute(func() (interface{}, error) {
		err := p.writer.WriteMessages(ctx, kafkaMsg)
		if err != nil {
			p.metrics.ErrorsTotal.Add(1)
			return nil, err
		}
		p.metrics.MessagesPublished.Add(1)
		p.metrics.BytesPublished.Add(int64(len(value)))
		return nil, nil
	})

	if errors.Is(err, gobreaker.ErrOpenState) {
		return ErrCircuitOpen
	}

	return err
}

// PublishBatch publishes multiple messages in a single batch.
// This is the preferred method for high-throughput ERP workloads.
func (p *Producer) PublishBatch(ctx context.Context, messages []Message) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}

	if len(messages) == 0 {
		return nil
	}

	kafkaMsgs := make([]kafka.Message, 0, len(messages))
	var totalBytes int64

	for _, msg := range messages {
		value, err := marshal(msg.Value)
		if err != nil {
			return fmt.Errorf("marshal message: %w", err)
		}

		kafkaMsg := kafka.Message{
			Key:   []byte(msg.Key),
			Value: value,
		}

		if len(msg.Headers) > 0 {
			kafkaMsg.Headers = make([]kafka.Header, 0, len(msg.Headers))
			for k, v := range msg.Headers {
				kafkaMsg.Headers = append(kafkaMsg.Headers, kafka.Header{
					Key:   k,
					Value: []byte(v),
				})
			}
		}

		totalBytes += int64(len(value))
		kafkaMsgs = append(kafkaMsgs, kafkaMsg)
	}

	_, err := p.cb.Execute(func() (interface{}, error) {
		err := p.writer.WriteMessages(ctx, kafkaMsgs...)
		if err != nil {
			p.metrics.ErrorsTotal.Add(int64(len(messages)))
			return nil, err
		}
		p.metrics.MessagesPublished.Add(int64(len(messages)))
		p.metrics.BatchesPublished.Add(1)
		p.metrics.BytesPublished.Add(totalBytes)
		return nil, nil
	})

	if errors.Is(err, gobreaker.ErrOpenState) {
		return ErrCircuitOpen
	}

	return err
}

// PublishAsync publishes a message asynchronously using a channel.
// Returns immediately. Errors are not returned - use metrics to monitor.
func (p *Producer) PublishAsync(msg Message) error {
	if p.closed.Load() {
		return ErrProducerClosed
	}

	value, err := marshal(msg.Value)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	kafkaMsg := kafka.Message{
		Key:   []byte(msg.Key),
		Value: value,
	}

	if len(msg.Headers) > 0 {
		kafkaMsg.Headers = make([]kafka.Header, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			kafkaMsg.Headers = append(kafkaMsg.Headers, kafka.Header{
				Key:   k,
				Value: []byte(v),
			})
		}
	}

	// Fire and forget - async mode handles batching
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := p.writer.WriteMessages(ctx, kafkaMsg); err != nil {
			p.metrics.ErrorsTotal.Add(1)
		} else {
			p.metrics.MessagesPublished.Add(1)
			p.metrics.BytesPublished.Add(int64(len(value)))
		}
	}()

	return nil
}

// Stats returns writer statistics.
func (p *Producer) Stats() kafka.WriterStats {
	return p.writer.Stats()
}

// Metrics returns producer metrics.
func (p *Producer) Metrics() ProducerMetrics {
	return ProducerMetrics{
		MessagesPublished: atomic.Int64{},
		BatchesPublished:  atomic.Int64{},
		BytesPublished:    atomic.Int64{},
		ErrorsTotal:       atomic.Int64{},
		CircuitOpens:      atomic.Int64{},
	}
}

// GetMetrics returns current metrics values.
func (p *Producer) GetMetrics() map[string]int64 {
	return map[string]int64{
		"messages_published": p.metrics.MessagesPublished.Load(),
		"batches_published":  p.metrics.BatchesPublished.Load(),
		"bytes_published":    p.metrics.BytesPublished.Load(),
		"errors_total":       p.metrics.ErrorsTotal.Load(),
		"circuit_opens":      p.metrics.CircuitOpens.Load(),
	}
}

// Close closes the producer gracefully.
func (p *Producer) Close() error {
	if p.closed.Swap(true) {
		return nil // Already closed
	}
	return p.writer.Close()
}

// marshal converts a value to bytes.
func marshal(v interface{}) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		return json.Marshal(v)
	}
}
