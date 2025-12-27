package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/viper"
)

type Config struct {
	Server ServerConfig
	Kafka  KafkaConfig
	Redis  RedisConfig
}

type ServerConfig struct {
	Port         int
	Prefork      bool
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	BodyLimit    int
	Concurrency  int
}

type KafkaConfig struct {
	Brokers      []string
	Topic        string
	BatchSize    int
	BatchTimeout time.Duration
	RequiredAcks int
	Async        bool
}

type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
}

// Load reads configuration from environment variables and .env file
func Load() (*Config, error) {
	// Load .env file if it exists (ignore error if file doesn't exist)
	_ = godotenv.Load()

	// Set default values
	viper.SetDefault("SERVER_PORT", 8181)
	viper.SetDefault("SERVER_PREFORK", true)
	viper.SetDefault("SERVER_READ_TIMEOUT", "30s")
	viper.SetDefault("SERVER_WRITE_TIMEOUT", "30s")
	viper.SetDefault("SERVER_BODY_LIMIT", 4*1024*1024) // 4MB
	viper.SetDefault("SERVER_CONCURRENCY", 256*1024)

	viper.SetDefault("KAFKA_BROKERS", "localhost:9092")
	viper.SetDefault("KAFKA_TOPIC", "events.raw")
	viper.SetDefault("KAFKA_BATCH_SIZE", 1000)
	viper.SetDefault("KAFKA_BATCH_TIMEOUT", "10ms")
	viper.SetDefault("KAFKA_REQUIRED_ACKS", 1)
	viper.SetDefault("KAFKA_ASYNC", true)

	viper.SetDefault("REDIS_ADDR", "localhost:6379")
	viper.SetDefault("REDIS_PASSWORD", "")
	viper.SetDefault("REDIS_DB", 0)
	viper.SetDefault("REDIS_POOL_SIZE", 50)
	viper.SetDefault("REDIS_MIN_IDLE_CONNS", 10)
	viper.SetDefault("REDIS_MAX_RETRIES", 3)

	// Bind environment variables
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Parse configuration
	cfg := &Config{
		Server: ServerConfig{
			Port:         viper.GetInt("SERVER_PORT"),
			Prefork:      viper.GetBool("SERVER_PREFORK"),
			ReadTimeout:  viper.GetDuration("SERVER_READ_TIMEOUT"),
			WriteTimeout: viper.GetDuration("SERVER_WRITE_TIMEOUT"),
			BodyLimit:    viper.GetInt("SERVER_BODY_LIMIT"),
			Concurrency:  viper.GetInt("SERVER_CONCURRENCY"),
		},
		Kafka: KafkaConfig{
			Brokers:      parseBrokers(viper.GetString("KAFKA_BROKERS")),
			Topic:        viper.GetString("KAFKA_TOPIC"),
			BatchSize:    viper.GetInt("KAFKA_BATCH_SIZE"),
			BatchTimeout: viper.GetDuration("KAFKA_BATCH_TIMEOUT"),
			RequiredAcks: viper.GetInt("KAFKA_REQUIRED_ACKS"),
			Async:        viper.GetBool("KAFKA_ASYNC"),
		},
		Redis: RedisConfig{
			Addr:         viper.GetString("REDIS_ADDR"),
			Password:     viper.GetString("REDIS_PASSWORD"),
			DB:           viper.GetInt("REDIS_DB"),
			PoolSize:     viper.GetInt("REDIS_POOL_SIZE"),
			MinIdleConns: viper.GetInt("REDIS_MIN_IDLE_CONNS"),
			MaxRetries:   viper.GetInt("REDIS_MAX_RETRIES"),
		},
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return cfg, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Server.Port < 1 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka brokers cannot be empty")
	}

	if c.Kafka.Topic == "" {
		return fmt.Errorf("kafka topic cannot be empty")
	}

	if c.Redis.Addr == "" {
		return fmt.Errorf("redis address cannot be empty")
	}

	return nil
}

// parseBrokers splits comma-separated broker addresses
func parseBrokers(brokers string) []string {
	parts := strings.Split(brokers, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
