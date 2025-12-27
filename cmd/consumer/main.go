package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"

	"flashpipe/internal/config"
	"flashpipe/internal/kafka"
	"flashpipe/internal/logging"
	"flashpipe/internal/model"
)

func main() {
	// Initialize logger
	logger := logging.New(logging.Config{
		Level:  os.Getenv("LOG_LEVEL"),
		Pretty: os.Getenv("LOG_PRETTY") == "true",
	})
	log := logger.WithComponent("consumer")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load configuration", err)
	}

	log.Info("starting consumer",
		logging.F("topic", cfg.Kafka.Topic),
		logging.F("brokers", cfg.Kafka.Brokers),
	)

	// Create message handler
	handler := func(ctx context.Context, msg kafka.Message) error {
		// Parse event
		var evt model.Event
		if data, ok := msg.Value.([]byte); ok {
			if err := json.Unmarshal(data, &evt); err != nil {
				log.Error("failed to parse event", err)
				return err
			}
		}

		// Process based on workflow type
		switch evt.Workflow {
		case model.WorkflowPayroll:
			return processPayroll(ctx, log, evt)
		case model.WorkflowAttendance:
			return processAttendance(ctx, log, evt)
		case model.WorkflowTrialBalance:
			return processTrialBalance(ctx, log, evt)
		default:
			return processGeneral(ctx, log, evt)
		}
	}

	// Create consumer
	consumerCfg := kafka.DefaultConsumerConfig(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		"flashpipe-consumer-group",
	)
	consumer := kafka.NewConsumer(consumerCfg, handler)

	// Start consumer with multiple workers
	ctx, cancel := context.WithCancel(context.Background())
	workers := 4
	if err := consumer.StartWithWorkers(ctx, workers); err != nil {
		log.Fatal("failed to start consumer", err)
	}

	log.Info("consumer started", logging.F("workers", workers))

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Info("shutdown signal received")
	cancel()

	if err := consumer.Stop(); err != nil {
		log.Error("consumer stop error", err)
	}

	log.Info("consumer stopped", logging.F("metrics", consumer.GetMetrics()))
}

func processPayroll(ctx context.Context, log *logging.Logger, evt model.Event) error {
	log.Info("processing payroll event",
		logging.F("event_id", evt.ID),
		logging.F("event_type", evt.EventType),
		logging.F("tenant_id", evt.TenantID),
	)

	// TODO: Implement payroll processing logic
	// - Calculate salary
	// - Apply deductions
	// - Generate payslip
	// - Update database

	return nil
}

func processAttendance(ctx context.Context, log *logging.Logger, evt model.Event) error {
	log.Info("processing attendance event",
		logging.F("event_id", evt.ID),
		logging.F("event_type", evt.EventType),
		logging.F("tenant_id", evt.TenantID),
	)

	// TODO: Implement attendance processing logic
	// - Validate check-in/out times
	// - Calculate work hours
	// - Update attendance records

	return nil
}

func processTrialBalance(ctx context.Context, log *logging.Logger, evt model.Event) error {
	log.Info("processing trial balance event",
		logging.F("event_id", evt.ID),
		logging.F("event_type", evt.EventType),
		logging.F("tenant_id", evt.TenantID),
	)

	// TODO: Implement trial balance processing logic
	// - Validate entries
	// - Calculate totals
	// - Generate report

	return nil
}

func processGeneral(ctx context.Context, log *logging.Logger, evt model.Event) error {
	log.Info("processing general event",
		logging.F("event_id", evt.ID),
		logging.F("event_type", evt.EventType),
		logging.F("tenant_id", evt.TenantID),
	)

	return nil
}
