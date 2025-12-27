package handler

import (
	"context"
	"time"

	"flashpipe/internal/kafka"
	"flashpipe/internal/logging"
	"flashpipe/internal/middleware"
	"flashpipe/internal/model"
	"flashpipe/internal/redis"
	"flashpipe/internal/validate"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

const (
	// IdempotencyKeyTTL is the duration for which idempotency keys are kept.
	IdempotencyKeyTTL = 24 * time.Hour
)

// IngestHandler handles event ingestion requests.
type IngestHandler struct {
	producer  *kafka.Producer
	redis     *redis.Client
	validator validate.Validator
	logger    *logging.Logger
}

// NewIngestHandler creates a new ingestion handler.
func NewIngestHandler(
	producer *kafka.Producer,
	redisClient *redis.Client,
	validator validate.Validator,
	logger *logging.Logger,
) *IngestHandler {
	return &IngestHandler{
		producer:  producer,
		redis:     redisClient,
		validator: validator,
		logger:    logger.WithComponent("ingest_handler"),
	}
}

// IngestSingle handles single event ingestion.
// POST /api/v1/events
func (h *IngestHandler) IngestSingle(c *fiber.Ctx) error {
	ctx := c.Context()
	requestID, _ := c.Locals(middleware.RequestIDKey).(string)
	log := h.logger.WithRequestID(requestID)

	var evt model.Event
	if err := c.BodyParser(&evt); err != nil {
		log.Warn("invalid request body", logging.F("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(model.ErrorResponse{
			Code:      "INVALID_BODY",
			Message:   "Failed to parse request body",
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Assign ID if not provided
	if evt.ID == "" {
		evt.ID = uuid.New().String()
	}

	// Validate event
	if err := h.validator.Validate(ctx, evt); err != nil {
		log.Warn("validation failed", logging.F("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(model.ErrorResponse{
			Code:      "VALIDATION_ERROR",
			Message:   err.Error(),
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Check idempotency
	if h.redis != nil {
		isNew, err := h.redis.SetIdempotencyKey(ctx, evt.IdempotencyKey, IdempotencyKeyTTL)
		if err != nil {
			log.Error("idempotency check failed", err)
			// Continue without idempotency check if Redis fails
		} else if !isNew {
			log.Debug("duplicate event detected", logging.F("idempotency_key", evt.IdempotencyKey))
			return c.Status(fiber.StatusConflict).JSON(model.ErrorResponse{
				Code:      "DUPLICATE_EVENT",
				Message:   "Event with this idempotency key already processed",
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}

	// Publish to Kafka
	msg := kafka.Message{
		Key:   evt.TenantID,
		Value: evt,
		Headers: map[string]string{
			"workflow":       string(evt.Workflow),
			"event_type":     evt.EventType,
			"correlation_id": evt.Metadata.CorrelationID,
			"request_id":     requestID,
		},
	}

	if err := h.producer.Publish(ctx, msg); err != nil {
		log.Error("failed to publish event", err, logging.F("event_id", evt.ID))
		return c.Status(fiber.StatusInternalServerError).JSON(model.ErrorResponse{
			Code:      "PUBLISH_ERROR",
			Message:   "Failed to publish event",
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Record metrics
	middleware.RecordEventIngested(string(evt.Workflow), evt.TenantID, 1)

	log.Info("event ingested",
		logging.F("event_id", evt.ID),
		logging.F("workflow", evt.Workflow),
		logging.F("tenant_id", evt.TenantID),
	)

	return c.Status(fiber.StatusAccepted).JSON(model.IngestResponse{
		EventID:   evt.ID,
		Status:    "accepted",
		Timestamp: time.Now().UnixMilli(),
	})
}

// IngestBatch handles batch event ingestion.
// POST /api/v1/events/batch
func (h *IngestHandler) IngestBatch(c *fiber.Ctx) error {
	ctx := c.Context()
	requestID, _ := c.Locals(middleware.RequestIDKey).(string)
	log := h.logger.WithRequestID(requestID)

	var batch model.BatchEvent
	if err := c.BodyParser(&batch); err != nil {
		log.Warn("invalid batch request body", logging.F("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(model.ErrorResponse{
			Code:      "INVALID_BODY",
			Message:   "Failed to parse request body",
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Assign batch ID if not provided
	if batch.BatchID == "" {
		batch.BatchID = uuid.New().String()
	}

	// Validate batch
	if err := h.validator.Validate(ctx, batch); err != nil {
		log.Warn("batch validation failed", logging.F("error", err.Error()))
		return c.Status(fiber.StatusBadRequest).JSON(model.ErrorResponse{
			Code:      "VALIDATION_ERROR",
			Message:   err.Error(),
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Check batch idempotency
	if h.redis != nil {
		isNew, err := h.redis.SetIdempotencyKey(ctx, batch.IdempotencyKey, IdempotencyKeyTTL)
		if err != nil {
			log.Error("batch idempotency check failed", err)
		} else if !isNew {
			log.Debug("duplicate batch detected", logging.F("idempotency_key", batch.IdempotencyKey))
			return c.Status(fiber.StatusConflict).JSON(model.ErrorResponse{
				Code:      "DUPLICATE_BATCH",
				Message:   "Batch with this idempotency key already processed",
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}

	// Prepare messages for batch publishing
	messages := make([]kafka.Message, 0, len(batch.Events))
	var rejectedIDs []string

	for i := range batch.Events {
		evt := &batch.Events[i]

		// Assign ID if not provided
		if evt.ID == "" {
			evt.ID = uuid.New().String()
		}

		// Validate individual event
		if err := h.validator.Validate(ctx, *evt); err != nil {
			rejectedIDs = append(rejectedIDs, evt.ID)
			continue
		}

		messages = append(messages, kafka.Message{
			Key:   evt.TenantID,
			Value: *evt,
			Headers: map[string]string{
				"workflow":       string(evt.Workflow),
				"event_type":     evt.EventType,
				"batch_id":       batch.BatchID,
				"correlation_id": evt.Metadata.CorrelationID,
				"request_id":     requestID,
			},
		})
	}

	// Publish batch to Kafka
	if len(messages) > 0 {
		if err := h.producer.PublishBatch(ctx, messages); err != nil {
			log.Error("failed to publish batch", err, logging.F("batch_id", batch.BatchID))
			return c.Status(fiber.StatusInternalServerError).JSON(model.ErrorResponse{
				Code:      "PUBLISH_ERROR",
				Message:   "Failed to publish batch",
				Timestamp: time.Now().UnixMilli(),
			})
		}
	}

	accepted := len(messages)
	rejected := len(rejectedIDs)

	// Record metrics
	middleware.RecordEventIngested(string(batch.Workflow), batch.TenantID, accepted)
	middleware.RecordBatchSize(string(batch.Workflow), len(batch.Events))

	log.Info("batch ingested",
		logging.F("batch_id", batch.BatchID),
		logging.F("workflow", batch.Workflow),
		logging.F("tenant_id", batch.TenantID),
		logging.F("accepted", accepted),
		logging.F("rejected", rejected),
	)

	return c.Status(fiber.StatusAccepted).JSON(model.BatchIngestResponse{
		BatchID:     batch.BatchID,
		Accepted:    accepted,
		Rejected:    rejected,
		RejectedIDs: rejectedIDs,
		Status:      "accepted",
		Timestamp:   time.Now().UnixMilli(),
	})
}

// IngestAsync handles async event ingestion with immediate response.
// POST /api/v1/events/async
func (h *IngestHandler) IngestAsync(c *fiber.Ctx) error {
	requestID, _ := c.Locals(middleware.RequestIDKey).(string)
	log := h.logger.WithRequestID(requestID)

	var evt model.Event
	if err := c.BodyParser(&evt); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(model.ErrorResponse{
			Code:      "INVALID_BODY",
			Message:   "Failed to parse request body",
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Assign ID if not provided
	if evt.ID == "" {
		evt.ID = uuid.New().String()
	}

	// Quick validation
	if err := h.validator.Validate(context.Background(), evt); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(model.ErrorResponse{
			Code:      "VALIDATION_ERROR",
			Message:   err.Error(),
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Fire and forget
	msg := kafka.Message{
		Key:   evt.TenantID,
		Value: evt,
		Headers: map[string]string{
			"workflow":   string(evt.Workflow),
			"event_type": evt.EventType,
			"request_id": requestID,
			"async":      "true",
		},
	}

	if err := h.producer.PublishAsync(msg); err != nil {
		log.Error("failed to queue async event", err)
		return c.Status(fiber.StatusServiceUnavailable).JSON(model.ErrorResponse{
			Code:      "QUEUE_FULL",
			Message:   "System is busy, please retry",
			Timestamp: time.Now().UnixMilli(),
		})
	}

	// Record metrics
	middleware.RecordEventIngested(string(evt.Workflow), evt.TenantID, 1)

	return c.Status(fiber.StatusAccepted).JSON(model.IngestResponse{
		EventID:   evt.ID,
		Status:    "queued",
		Timestamp: time.Now().UnixMilli(),
	})
}
