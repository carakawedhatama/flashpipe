package logging

import (
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

// Logger wraps zerolog for structured logging with contextual fields.
type Logger struct {
	zl zerolog.Logger
}

// Config holds logger configuration.
type Config struct {
	Level      string
	Pretty     bool
	TimeFormat string
}

// New creates a new structured logger.
func New(cfg Config) *Logger {
	var output io.Writer = os.Stdout

	if cfg.Pretty {
		output = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}
	}

	level := parseLevel(cfg.Level)
	timeFormat := cfg.TimeFormat
	if timeFormat == "" {
		timeFormat = time.RFC3339Nano
	}

	zerolog.TimeFieldFormat = timeFormat

	zl := zerolog.New(output).
		Level(level).
		With().
		Timestamp().
		Caller().
		Logger()

	return &Logger{zl: zl}
}

// WithComponent returns a logger with a component field.
func (l *Logger) WithComponent(name string) *Logger {
	return &Logger{zl: l.zl.With().Str("component", name).Logger()}
}

// WithRequestID returns a logger with a request ID field.
func (l *Logger) WithRequestID(id string) *Logger {
	return &Logger{zl: l.zl.With().Str("request_id", id).Logger()}
}

// WithTenant returns a logger with a tenant ID field.
func (l *Logger) WithTenant(id string) *Logger {
	return &Logger{zl: l.zl.With().Str("tenant_id", id).Logger()}
}

// WithWorkflow returns a logger with a workflow type field.
func (l *Logger) WithWorkflow(wf string) *Logger {
	return &Logger{zl: l.zl.With().Str("workflow", wf).Logger()}
}

// Debug logs a debug message.
func (l *Logger) Debug(msg string, fields ...Field) {
	event := l.zl.Debug()
	applyFields(event, fields)
	event.Msg(msg)
}

// Info logs an info message.
func (l *Logger) Info(msg string, fields ...Field) {
	event := l.zl.Info()
	applyFields(event, fields)
	event.Msg(msg)
}

// Warn logs a warning message.
func (l *Logger) Warn(msg string, fields ...Field) {
	event := l.zl.Warn()
	applyFields(event, fields)
	event.Msg(msg)
}

// Error logs an error message.
func (l *Logger) Error(msg string, err error, fields ...Field) {
	event := l.zl.Error().Err(err)
	applyFields(event, fields)
	event.Msg(msg)
}

// Fatal logs a fatal message and exits.
func (l *Logger) Fatal(msg string, err error, fields ...Field) {
	event := l.zl.Fatal().Err(err)
	applyFields(event, fields)
	event.Msg(msg)
}

// Field represents a log field.
type Field struct {
	Key   string
	Value interface{}
}

// F creates a new field.
func F(key string, value interface{}) Field {
	return Field{Key: key, Value: value}
}

func applyFields(event *zerolog.Event, fields []Field) {
	for _, f := range fields {
		event.Interface(f.Key, f.Value)
	}
}

func parseLevel(level string) zerolog.Level {
	switch level {
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	case "fatal":
		return zerolog.FatalLevel
	default:
		return zerolog.InfoLevel
	}
}
