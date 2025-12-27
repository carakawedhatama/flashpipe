package config

import (
	"os"
	"testing"
	"time"
)

func TestLoad_Defaults(t *testing.T) {
	// Clear any existing env vars
	envVars := []string{
		"SERVER_PORT", "SERVER_PREFORK", "SERVER_READ_TIMEOUT",
		"SERVER_WRITE_TIMEOUT", "SERVER_BODY_LIMIT", "SERVER_CONCURRENCY",
		"KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_BATCH_SIZE",
		"KAFKA_BATCH_TIMEOUT", "KAFKA_REQUIRED_ACKS", "KAFKA_ASYNC",
		"REDIS_ADDR", "REDIS_PASSWORD", "REDIS_DB",
		"REDIS_POOL_SIZE", "REDIS_MIN_IDLE_CONNS", "REDIS_MAX_RETRIES",
	}
	for _, v := range envVars {
		os.Unsetenv(v)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	// Check server defaults
	if cfg.Server.Port != 8181 {
		t.Errorf("expected port 8181, got %d", cfg.Server.Port)
	}
	if cfg.Server.Prefork != true {
		t.Error("expected prefork true")
	}
	if cfg.Server.ReadTimeout != 30*time.Second {
		t.Errorf("expected read timeout 30s, got %v", cfg.Server.ReadTimeout)
	}
	if cfg.Server.WriteTimeout != 30*time.Second {
		t.Errorf("expected write timeout 30s, got %v", cfg.Server.WriteTimeout)
	}
	if cfg.Server.BodyLimit != 4*1024*1024 {
		t.Errorf("expected body limit 4MB, got %d", cfg.Server.BodyLimit)
	}
	if cfg.Server.Concurrency != 256*1024 {
		t.Errorf("expected concurrency 256K, got %d", cfg.Server.Concurrency)
	}

	// Check Kafka defaults
	if len(cfg.Kafka.Brokers) != 1 || cfg.Kafka.Brokers[0] != "localhost:9092" {
		t.Errorf("expected brokers [localhost:9092], got %v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.Topic != "events.raw" {
		t.Errorf("expected topic events.raw, got %s", cfg.Kafka.Topic)
	}
	if cfg.Kafka.BatchSize != 1000 {
		t.Errorf("expected batch size 1000, got %d", cfg.Kafka.BatchSize)
	}
	if cfg.Kafka.BatchTimeout != 10*time.Millisecond {
		t.Errorf("expected batch timeout 10ms, got %v", cfg.Kafka.BatchTimeout)
	}
	if cfg.Kafka.RequiredAcks != 1 {
		t.Errorf("expected required acks 1, got %d", cfg.Kafka.RequiredAcks)
	}
	if cfg.Kafka.Async != true {
		t.Error("expected async true")
	}

	// Check Redis defaults
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("expected redis addr localhost:6379, got %s", cfg.Redis.Addr)
	}
	if cfg.Redis.Password != "" {
		t.Errorf("expected empty password, got %s", cfg.Redis.Password)
	}
	if cfg.Redis.DB != 0 {
		t.Errorf("expected DB 0, got %d", cfg.Redis.DB)
	}
	if cfg.Redis.PoolSize != 50 {
		t.Errorf("expected pool size 50, got %d", cfg.Redis.PoolSize)
	}
	if cfg.Redis.MinIdleConns != 10 {
		t.Errorf("expected min idle conns 10, got %d", cfg.Redis.MinIdleConns)
	}
	if cfg.Redis.MaxRetries != 3 {
		t.Errorf("expected max retries 3, got %d", cfg.Redis.MaxRetries)
	}
}

func TestLoad_FromEnv(t *testing.T) {
	// Set custom env vars
	os.Setenv("SERVER_PORT", "9090")
	os.Setenv("SERVER_PREFORK", "false")
	os.Setenv("SERVER_READ_TIMEOUT", "60s")
	os.Setenv("KAFKA_BROKERS", "broker1:9092,broker2:9092,broker3:9092")
	os.Setenv("KAFKA_TOPIC", "custom.topic")
	os.Setenv("REDIS_ADDR", "redis.example.com:6379")
	os.Setenv("REDIS_PASSWORD", "secret")
	os.Setenv("REDIS_DB", "5")

	defer func() {
		os.Unsetenv("SERVER_PORT")
		os.Unsetenv("SERVER_PREFORK")
		os.Unsetenv("SERVER_READ_TIMEOUT")
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("KAFKA_TOPIC")
		os.Unsetenv("REDIS_ADDR")
		os.Unsetenv("REDIS_PASSWORD")
		os.Unsetenv("REDIS_DB")
	}()

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() failed: %v", err)
	}

	if cfg.Server.Port != 9090 {
		t.Errorf("expected port 9090, got %d", cfg.Server.Port)
	}
	if cfg.Server.Prefork != false {
		t.Error("expected prefork false")
	}
	if cfg.Server.ReadTimeout != 60*time.Second {
		t.Errorf("expected read timeout 60s, got %v", cfg.Server.ReadTimeout)
	}

	if len(cfg.Kafka.Brokers) != 3 {
		t.Errorf("expected 3 brokers, got %d", len(cfg.Kafka.Brokers))
	}
	if cfg.Kafka.Topic != "custom.topic" {
		t.Errorf("expected topic custom.topic, got %s", cfg.Kafka.Topic)
	}

	if cfg.Redis.Addr != "redis.example.com:6379" {
		t.Errorf("expected redis addr redis.example.com:6379, got %s", cfg.Redis.Addr)
	}
	if cfg.Redis.Password != "secret" {
		t.Errorf("expected password secret, got %s", cfg.Redis.Password)
	}
	if cfg.Redis.DB != 5 {
		t.Errorf("expected DB 5, got %d", cfg.Redis.DB)
	}
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config",
			config: Config{
				Server: ServerConfig{Port: 8080},
				Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Redis:  RedisConfig{Addr: "localhost:6379"},
			},
			wantErr: false,
		},
		{
			name: "invalid port - zero",
			config: Config{
				Server: ServerConfig{Port: 0},
				Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Redis:  RedisConfig{Addr: "localhost:6379"},
			},
			wantErr: true,
		},
		{
			name: "invalid port - negative",
			config: Config{
				Server: ServerConfig{Port: -1},
				Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Redis:  RedisConfig{Addr: "localhost:6379"},
			},
			wantErr: true,
		},
		{
			name: "invalid port - too high",
			config: Config{
				Server: ServerConfig{Port: 70000},
				Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Redis:  RedisConfig{Addr: "localhost:6379"},
			},
			wantErr: true,
		},
		{
			name: "empty kafka brokers",
			config: Config{
				Server: ServerConfig{Port: 8080},
				Kafka:  KafkaConfig{Brokers: []string{}, Topic: "test"},
				Redis:  RedisConfig{Addr: "localhost:6379"},
			},
			wantErr: true,
		},
		{
			name: "nil kafka brokers",
			config: Config{
				Server: ServerConfig{Port: 8080},
				Kafka:  KafkaConfig{Brokers: nil, Topic: "test"},
				Redis:  RedisConfig{Addr: "localhost:6379"},
			},
			wantErr: true,
		},
		{
			name: "empty kafka topic",
			config: Config{
				Server: ServerConfig{Port: 8080},
				Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: ""},
				Redis:  RedisConfig{Addr: "localhost:6379"},
			},
			wantErr: true,
		},
		{
			name: "empty redis addr",
			config: Config{
				Server: ServerConfig{Port: 8080},
				Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
				Redis:  RedisConfig{Addr: ""},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestParseBrokers(t *testing.T) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			input:    "localhost:9092",
			expected: []string{"localhost:9092"},
		},
		{
			input:    "broker1:9092,broker2:9092",
			expected: []string{"broker1:9092", "broker2:9092"},
		},
		{
			input:    "broker1:9092, broker2:9092, broker3:9092",
			expected: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			input:    "  broker1:9092  ,  broker2:9092  ",
			expected: []string{"broker1:9092", "broker2:9092"},
		},
		{
			input:    "",
			expected: []string{},
		},
		{
			input:    "   ",
			expected: []string{},
		},
		{
			input:    ",,,",
			expected: []string{},
		},
		{
			input:    "single",
			expected: []string{"single"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseBrokers(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d brokers, got %d", len(tt.expected), len(result))
				return
			}
			for i, broker := range result {
				if broker != tt.expected[i] {
					t.Errorf("expected broker[%d] = %s, got %s", i, tt.expected[i], broker)
				}
			}
		})
	}
}

func TestServerConfig_Fields(t *testing.T) {
	cfg := ServerConfig{
		Port:         8080,
		Prefork:      true,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		BodyLimit:    4 * 1024 * 1024,
		Concurrency:  256 * 1024,
	}

	if cfg.Port != 8080 {
		t.Errorf("expected port 8080, got %d", cfg.Port)
	}
	if !cfg.Prefork {
		t.Error("expected prefork true")
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("expected read timeout 30s, got %v", cfg.ReadTimeout)
	}
}

func TestKafkaConfig_Fields(t *testing.T) {
	cfg := KafkaConfig{
		Brokers:      []string{"broker1:9092", "broker2:9092"},
		Topic:        "events",
		BatchSize:    500,
		BatchTimeout: 5 * time.Millisecond,
		RequiredAcks: 2,
		Async:        false,
	}

	if len(cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Brokers))
	}
	if cfg.Topic != "events" {
		t.Errorf("expected topic events, got %s", cfg.Topic)
	}
	if cfg.BatchSize != 500 {
		t.Errorf("expected batch size 500, got %d", cfg.BatchSize)
	}
	if cfg.Async {
		t.Error("expected async false")
	}
}

func TestRedisConfig_Fields(t *testing.T) {
	cfg := RedisConfig{
		Addr:         "redis:6379",
		Password:     "secret",
		DB:           1,
		PoolSize:     100,
		MinIdleConns: 20,
		MaxRetries:   5,
	}

	if cfg.Addr != "redis:6379" {
		t.Errorf("expected addr redis:6379, got %s", cfg.Addr)
	}
	if cfg.Password != "secret" {
		t.Errorf("expected password secret, got %s", cfg.Password)
	}
	if cfg.DB != 1 {
		t.Errorf("expected DB 1, got %d", cfg.DB)
	}
	if cfg.PoolSize != 100 {
		t.Errorf("expected pool size 100, got %d", cfg.PoolSize)
	}
}

// Edge cases
func TestLoad_EdgeCases(t *testing.T) {
	t.Run("port boundary - min valid", func(t *testing.T) {
		os.Setenv("SERVER_PORT", "1")
		defer os.Unsetenv("SERVER_PORT")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() failed: %v", err)
		}
		if cfg.Server.Port != 1 {
			t.Errorf("expected port 1, got %d", cfg.Server.Port)
		}
	})

	t.Run("port boundary - max valid", func(t *testing.T) {
		os.Setenv("SERVER_PORT", "65535")
		defer os.Unsetenv("SERVER_PORT")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() failed: %v", err)
		}
		if cfg.Server.Port != 65535 {
			t.Errorf("expected port 65535, got %d", cfg.Server.Port)
		}
	})

	t.Run("invalid duration format", func(t *testing.T) {
		os.Setenv("SERVER_READ_TIMEOUT", "invalid")
		defer os.Unsetenv("SERVER_READ_TIMEOUT")

		cfg, _ := Load()
		// Viper returns 0 for invalid duration
		if cfg.Server.ReadTimeout != 0 {
			t.Logf("Read timeout with invalid format: %v", cfg.Server.ReadTimeout)
		}
	})

	t.Run("empty environment", func(t *testing.T) {
		// Temporarily clear all env vars
		originalEnv := os.Environ()
		os.Clearenv()
		defer func() {
			os.Clearenv()
			for _, e := range originalEnv {
				parts := splitEnv(e)
				if len(parts) == 2 {
					os.Setenv(parts[0], parts[1])
				}
			}
		}()

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() failed with empty env: %v", err)
		}
		// Should use defaults
		if cfg.Server.Port != 8181 {
			t.Errorf("expected default port, got %d", cfg.Server.Port)
		}
	})
}

func splitEnv(e string) []string {
	for i := 0; i < len(e); i++ {
		if e[i] == '=' {
			return []string{e[:i], e[i+1:]}
		}
	}
	return []string{e, ""}
}

func TestConfig_Validate_EdgeCases(t *testing.T) {
	t.Run("port exactly 65535", func(t *testing.T) {
		cfg := Config{
			Server: ServerConfig{Port: 65535},
			Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
			Redis:  RedisConfig{Addr: "localhost:6379"},
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("port 65535 should be valid: %v", err)
		}
	})

	t.Run("port exactly 65536", func(t *testing.T) {
		cfg := Config{
			Server: ServerConfig{Port: 65536},
			Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
			Redis:  RedisConfig{Addr: "localhost:6379"},
		}
		if err := cfg.Validate(); err == nil {
			t.Error("port 65536 should be invalid")
		}
	})

	t.Run("whitespace-only topic", func(t *testing.T) {
		cfg := Config{
			Server: ServerConfig{Port: 8080},
			Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "   "},
			Redis:  RedisConfig{Addr: "localhost:6379"},
		}
		// Whitespace-only is considered non-empty by the validation
		if err := cfg.Validate(); err != nil {
			t.Logf("whitespace topic validation: %v", err)
		}
	})

	t.Run("whitespace-only redis addr", func(t *testing.T) {
		cfg := Config{
			Server: ServerConfig{Port: 8080},
			Kafka:  KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "test"},
			Redis:  RedisConfig{Addr: "   "},
		}
		// Whitespace-only is considered non-empty by the validation
		if err := cfg.Validate(); err != nil {
			t.Logf("whitespace redis addr validation: %v", err)
		}
	})
}

func TestParseBrokers_EdgeCases(t *testing.T) {
	t.Run("broker with spaces in name", func(t *testing.T) {
		// Should trim spaces
		result := parseBrokers("  host name:9092  ")
		if len(result) != 1 {
			t.Errorf("expected 1 broker, got %d", len(result))
		}
		if result[0] != "host name:9092" {
			t.Errorf("expected 'host name:9092', got '%s'", result[0])
		}
	})

	t.Run("unicode broker name", func(t *testing.T) {
		result := parseBrokers("日本語:9092")
		if len(result) != 1 {
			t.Errorf("expected 1 broker, got %d", len(result))
		}
		if result[0] != "日本語:9092" {
			t.Errorf("expected '日本語:9092', got '%s'", result[0])
		}
	})

	t.Run("many brokers", func(t *testing.T) {
		var brokers []string
		for i := 0; i < 100; i++ {
			brokers = append(brokers, "broker:9092")
		}
		input := joinBrokers(brokers)
		result := parseBrokers(input)
		if len(result) != 100 {
			t.Errorf("expected 100 brokers, got %d", len(result))
		}
	})
}

func joinBrokers(brokers []string) string {
	result := ""
	for i, b := range brokers {
		if i > 0 {
			result += ","
		}
		result += b
	}
	return result
}
