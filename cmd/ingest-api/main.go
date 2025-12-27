package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"flashpipe/internal/config"
	"flashpipe/internal/handler"
	"flashpipe/internal/kafka"
	"flashpipe/internal/logging"
	"flashpipe/internal/middleware"
	"flashpipe/internal/redis"
	"flashpipe/internal/validate"

	"github.com/gofiber/adaptor/v2"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const Version = "1.0.0"

func main() {
	// Initialize logger
	logger := logging.New(logging.Config{
		Level:  os.Getenv("LOG_LEVEL"),
		Pretty: os.Getenv("LOG_PRETTY") == "true",
	})
	log := logger.WithComponent("main")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load configuration", err)
	}

	log.Info("configuration loaded",
		logging.F("port", cfg.Server.Port),
		logging.F("kafka_brokers", cfg.Kafka.Brokers),
	)

	// Initialize Redis client
	var redisClient *redis.Client
	if cfg.Redis.Addr != "" {
		redisClient, err = redis.NewClient(redis.Config{
			Addr:         cfg.Redis.Addr,
			Password:     cfg.Redis.Password,
			DB:           cfg.Redis.DB,
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConns,
			MaxRetries:   cfg.Redis.MaxRetries,
			Prefix:       "flashpipe",
		})
		if err != nil {
			log.Warn("failed to connect to Redis, continuing without Redis",
				logging.F("error", err.Error()),
			)
			redisClient = nil
		} else {
			log.Info("redis connected", logging.F("addr", cfg.Redis.Addr))
		}
	}

	// Initialize Kafka producer
	producerCfg := kafka.DefaultProducerConfig(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	producerCfg.BatchSize = cfg.Kafka.BatchSize
	producerCfg.BatchTimeout = cfg.Kafka.BatchTimeout
	producerCfg.Async = cfg.Kafka.Async
	producer := kafka.NewProducer(producerCfg)
	log.Info("kafka producer initialized",
		logging.F("topic", cfg.Kafka.Topic),
		logging.F("batch_size", cfg.Kafka.BatchSize),
	)

	// Initialize validator (singleton)
	validator := validate.NewGoValidator()

	// Initialize handlers
	ingestHandler := handler.NewIngestHandler(producer, redisClient, validator, logger)
	healthHandler := handler.NewHealthHandler(Version, redisClient, producer)

	// Initialize rate limiter
	rateLimiter := middleware.NewSimpleRateLimiter(
		cfg.Server.Concurrency/10, // RPS
		cfg.Server.Concurrency,    // Burst
	)

	// Configure Fiber app
	app := fiber.New(fiber.Config{
		Prefork:               cfg.Server.Prefork,
		ReadTimeout:           cfg.Server.ReadTimeout,
		WriteTimeout:          cfg.Server.WriteTimeout,
		BodyLimit:             cfg.Server.BodyLimit,
		Concurrency:           cfg.Server.Concurrency,
		DisableStartupMessage: true,
		ErrorHandler:          customErrorHandler(logger),
	})

	// Global middleware
	app.Use(middleware.RequestID())
	app.Use(middleware.Recovery(logger))
	app.Use(middleware.Metrics())
	app.Use(middleware.CORS())
	app.Use(compress.New(compress.Config{
		Level: compress.LevelBestSpeed,
	}))

	// Health endpoints (no rate limiting)
	app.Get("/health/live", healthHandler.Liveness)
	app.Get("/health/ready", healthHandler.Readiness)
	app.Get("/health/metrics", healthHandler.Metrics)

	// Prometheus metrics endpoint
	app.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	// API v1 routes with rate limiting
	v1 := app.Group("/api/v1", middleware.RateLimit(rateLimiter))

	// Event ingestion endpoints
	v1.Post("/events", ingestHandler.IngestSingle)
	v1.Post("/events/batch", ingestHandler.IngestBatch)
	v1.Post("/events/async", ingestHandler.IngestAsync)

	// Graceful shutdown
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-shutdownChan
		log.Info("shutdown signal received, gracefully shutting down...")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Shutdown server
		if err := app.ShutdownWithContext(ctx); err != nil {
			log.Error("server shutdown error", err)
		}

		// Close producer
		if err := producer.Close(); err != nil {
			log.Error("producer close error", err)
		}

		// Close Redis
		if redisClient != nil {
			if err := redisClient.Close(); err != nil {
				log.Error("redis close error", err)
			}
		}

		log.Info("shutdown complete")
	}()

	// Start server
	log.Info("starting server",
		logging.F("port", cfg.Server.Port),
		logging.F("version", Version),
		logging.F("prefork", cfg.Server.Prefork),
	)

	if err := app.Listen(fmt.Sprintf(":%d", cfg.Server.Port)); err != nil {
		log.Fatal("server failed", err)
	}
}

// customErrorHandler handles unhandled errors.
func customErrorHandler(logger *logging.Logger) fiber.ErrorHandler {
	return func(c *fiber.Ctx, err error) error {
		code := fiber.StatusInternalServerError

		if e, ok := err.(*fiber.Error); ok {
			code = e.Code
		}

		requestID, _ := c.Locals(middleware.RequestIDKey).(string)
		logger.WithRequestID(requestID).Error("request error", err,
			logging.F("path", c.Path()),
			logging.F("method", c.Method()),
			logging.F("status", code),
		)

		return c.Status(code).JSON(fiber.Map{
			"code":      "ERROR",
			"message":   err.Error(),
			"timestamp": time.Now().UnixMilli(),
		})
	}
}
