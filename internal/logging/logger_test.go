package logging

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name   string
		config Config
	}{
		{
			name:   "default config",
			config: Config{},
		},
		{
			name: "debug level",
			config: Config{
				Level: "debug",
			},
		},
		{
			name: "error level",
			config: Config{
				Level: "error",
			},
		},
		{
			name: "pretty output",
			config: Config{
				Pretty: true,
			},
		},
		{
			name: "custom time format",
			config: Config{
				TimeFormat: "2006-01-02",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := New(tt.config)
			if logger == nil {
				t.Error("expected logger to be created")
			}
		})
	}
}

func TestLogger_WithComponent(t *testing.T) {
	logger := New(Config{Level: "debug"})
	componentLogger := logger.WithComponent("test-component")

	if componentLogger == nil {
		t.Error("expected component logger to be created")
	}
	if componentLogger == logger {
		t.Error("expected new logger instance")
	}
}

func TestLogger_WithRequestID(t *testing.T) {
	logger := New(Config{Level: "debug"})
	requestLogger := logger.WithRequestID("req-123")

	if requestLogger == nil {
		t.Error("expected request logger to be created")
	}
}

func TestLogger_WithTenant(t *testing.T) {
	logger := New(Config{Level: "debug"})
	tenantLogger := logger.WithTenant("tenant-456")

	if tenantLogger == nil {
		t.Error("expected tenant logger to be created")
	}
}

func TestLogger_WithWorkflow(t *testing.T) {
	logger := New(Config{Level: "debug"})
	workflowLogger := logger.WithWorkflow("payroll")

	if workflowLogger == nil {
		t.Error("expected workflow logger to be created")
	}
}

func TestLogger_Chaining(t *testing.T) {
	logger := New(Config{Level: "debug"})

	chainedLogger := logger.
		WithComponent("api").
		WithRequestID("req-123").
		WithTenant("tenant-456").
		WithWorkflow("payroll")

	if chainedLogger == nil {
		t.Error("expected chained logger to be created")
	}
}

func TestField(t *testing.T) {
	field := F("key", "value")

	if field.Key != "key" {
		t.Errorf("expected key 'key', got '%s'", field.Key)
	}
	if field.Value != "value" {
		t.Errorf("expected value 'value', got '%v'", field.Value)
	}
}

func TestField_Types(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value interface{}
	}{
		{"string value", "str", "hello"},
		{"int value", "int", 42},
		{"float value", "float", 3.14},
		{"bool value", "bool", true},
		{"nil value", "nil", nil},
		{"slice value", "slice", []int{1, 2, 3}},
		{"map value", "map", map[string]int{"a": 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field := F(tt.key, tt.value)
			if field.Key != tt.key {
				t.Errorf("expected key '%s', got '%s'", tt.key, field.Key)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected zerolog.Level
	}{
		{"debug", zerolog.DebugLevel},
		{"info", zerolog.InfoLevel},
		{"warn", zerolog.WarnLevel},
		{"error", zerolog.ErrorLevel},
		{"fatal", zerolog.FatalLevel},
		{"", zerolog.InfoLevel},        // default
		{"unknown", zerolog.InfoLevel}, // default
		{"DEBUG", zerolog.InfoLevel},   // case sensitive
		{"Info", zerolog.InfoLevel},    // case sensitive
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseLevel(tt.input)
			if result != tt.expected {
				t.Errorf("parseLevel(%s) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

// Test actual log output
func TestLogger_Output(t *testing.T) {
	// Capture output
	var buf bytes.Buffer
	oldOutput := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	logger := New(Config{Level: "debug"})
	logger.Info("test message", F("key", "value"))

	w.Close()
	os.Stdout = oldOutput
	io.Copy(&buf, r)

	output := buf.String()

	// Should contain the message
	if !strings.Contains(output, "test message") {
		t.Errorf("expected output to contain 'test message', got: %s", output)
	}
}

func TestLogger_JSONOutput(t *testing.T) {
	// Create a logger that writes to a buffer
	var buf bytes.Buffer
	zl := zerolog.New(&buf).With().Timestamp().Logger()
	logger := &Logger{zl: zl}

	logger.Info("json test", F("number", 42), F("string", "hello"))

	// Parse the JSON output
	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse JSON output: %v", err)
	}

	if result["message"] != "json test" {
		t.Errorf("expected message 'json test', got '%v'", result["message"])
	}
	if result["number"] != float64(42) {
		t.Errorf("expected number 42, got '%v'", result["number"])
	}
	if result["string"] != "hello" {
		t.Errorf("expected string 'hello', got '%v'", result["string"])
	}
}

func TestLogger_Debug(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf).Level(zerolog.DebugLevel)
	logger := &Logger{zl: zl}

	logger.Debug("debug message", F("level", "debug"))

	if !strings.Contains(buf.String(), "debug message") {
		t.Error("expected debug message in output")
	}
}

func TestLogger_Info(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf).Level(zerolog.InfoLevel)
	logger := &Logger{zl: zl}

	logger.Info("info message", F("level", "info"))

	if !strings.Contains(buf.String(), "info message") {
		t.Error("expected info message in output")
	}
}

func TestLogger_Warn(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf).Level(zerolog.WarnLevel)
	logger := &Logger{zl: zl}

	logger.Warn("warn message", F("level", "warn"))

	if !strings.Contains(buf.String(), "warn message") {
		t.Error("expected warn message in output")
	}
}

func TestLogger_Error(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf).Level(zerolog.ErrorLevel)
	logger := &Logger{zl: zl}

	logger.Error("error message", nil, F("level", "error"))

	if !strings.Contains(buf.String(), "error message") {
		t.Error("expected error message in output")
	}
}

func TestLogger_ErrorWithError(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf).Level(zerolog.ErrorLevel)
	logger := &Logger{zl: zl}

	testErr := &testError{msg: "test error"}
	logger.Error("something failed", testErr)

	output := buf.String()
	if !strings.Contains(output, "something failed") {
		t.Error("expected error message in output")
	}
	if !strings.Contains(output, "test error") {
		t.Error("expected error details in output")
	}
}

type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}

func TestLogger_LevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf).Level(zerolog.WarnLevel)
	logger := &Logger{zl: zl}

	// Debug and Info should be filtered out
	logger.Debug("debug message")
	logger.Info("info message")

	if strings.Contains(buf.String(), "debug message") {
		t.Error("debug message should be filtered")
	}
	if strings.Contains(buf.String(), "info message") {
		t.Error("info message should be filtered")
	}

	// Warn and Error should pass
	logger.Warn("warn message")
	if !strings.Contains(buf.String(), "warn message") {
		t.Error("warn message should be logged")
	}
}

func TestLogger_MultipleFields(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf)
	logger := &Logger{zl: zl}

	logger.Info("multi-field test",
		F("field1", "value1"),
		F("field2", 42),
		F("field3", true),
		F("field4", []string{"a", "b"}),
	)

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if result["field1"] != "value1" {
		t.Error("expected field1 = value1")
	}
	if result["field2"] != float64(42) {
		t.Error("expected field2 = 42")
	}
	if result["field3"] != true {
		t.Error("expected field3 = true")
	}
}

func TestLogger_NoFields(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf)
	logger := &Logger{zl: zl}

	logger.Info("no fields message")

	if !strings.Contains(buf.String(), "no fields message") {
		t.Error("expected message without fields")
	}
}

func TestApplyFields(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf)
	event := zl.Info()

	fields := []Field{
		{Key: "string", Value: "hello"},
		{Key: "int", Value: 42},
		{Key: "bool", Value: true},
	}

	applyFields(event, fields)
	event.Msg("test")

	var result map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("failed to parse JSON: %v", err)
	}

	if result["string"] != "hello" {
		t.Error("expected string field")
	}
}

func TestApplyFields_Empty(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf)
	event := zl.Info()

	applyFields(event, []Field{})
	event.Msg("test")

	if !strings.Contains(buf.String(), "test") {
		t.Error("expected message with empty fields")
	}
}

func TestApplyFields_Nil(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf)
	event := zl.Info()

	applyFields(event, nil)
	event.Msg("test")

	if !strings.Contains(buf.String(), "test") {
		t.Error("expected message with nil fields")
	}
}

// Edge cases
func TestLogger_EdgeCases(t *testing.T) {
	var buf bytes.Buffer
	zl := zerolog.New(&buf)
	logger := &Logger{zl: zl}

	t.Run("empty message", func(t *testing.T) {
		buf.Reset()
		logger.Info("")
		// Should still log
	})

	t.Run("unicode message", func(t *testing.T) {
		buf.Reset()
		logger.Info("日本語メッセージ 🎉")
		if !strings.Contains(buf.String(), "日本語") {
			t.Error("expected unicode in output")
		}
	})

	t.Run("very long message", func(t *testing.T) {
		buf.Reset()
		longMsg := strings.Repeat("a", 10000)
		logger.Info(longMsg)
		if !strings.Contains(buf.String(), longMsg[:100]) {
			t.Error("expected long message in output")
		}
	})

	t.Run("special characters in message", func(t *testing.T) {
		buf.Reset()
		logger.Info("message with \"quotes\" and \\ backslash")
		// JSON should escape properly
		var result map[string]interface{}
		if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
			t.Errorf("should produce valid JSON: %v", err)
		}
	})

	t.Run("newlines in message", func(t *testing.T) {
		buf.Reset()
		logger.Info("line1\nline2\nline3")
		var result map[string]interface{}
		if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
			t.Errorf("should produce valid JSON with newlines: %v", err)
		}
	})

	t.Run("nil field value", func(t *testing.T) {
		buf.Reset()
		logger.Info("test", F("nil_field", nil))
		// Should not panic
	})

	t.Run("empty field key", func(t *testing.T) {
		buf.Reset()
		logger.Info("test", F("", "value"))
		// Should not panic
	})

	t.Run("complex nested value", func(t *testing.T) {
		buf.Reset()
		complex := map[string]interface{}{
			"nested": map[string]interface{}{
				"deep": map[string]interface{}{
					"value": 123,
				},
			},
			"array": []int{1, 2, 3},
		}
		logger.Info("complex", F("data", complex))
		// Should not panic
	})
}

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{}

	if cfg.Level != "" {
		t.Errorf("expected empty level, got '%s'", cfg.Level)
	}
	if cfg.Pretty != false {
		t.Error("expected Pretty to be false")
	}
	if cfg.TimeFormat != "" {
		t.Errorf("expected empty time format, got '%s'", cfg.TimeFormat)
	}
}
