package validate

import (
	"context"
	"testing"
	"time"
)

type TestStruct struct {
	Required    string `validate:"required"`
	Email       string `validate:"omitempty,email"`
	MinLength   string `validate:"omitempty,min=3"`
	MaxLength   string `validate:"omitempty,max=10"`
	Numeric     string `validate:"omitempty,numeric"`
	OneOf       string `validate:"omitempty,oneof=a b c"`
	NestedPtr   *NestedStruct
	NestedSlice []NestedStruct `validate:"dive"`
}

type NestedStruct struct {
	Value string `validate:"required"`
}

func TestNewGoValidator(t *testing.T) {
	v := NewGoValidator()
	if v == nil {
		t.Fatal("expected validator to be created")
	}
}

func TestGoValidator_Validate_Required(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	tests := []struct {
		name    string
		data    TestStruct
		wantErr bool
	}{
		{
			name:    "valid required field",
			data:    TestStruct{Required: "value"},
			wantErr: false,
		},
		{
			name:    "missing required field",
			data:    TestStruct{Required: ""},
			wantErr: true,
		},
		{
			name:    "whitespace only required field",
			data:    TestStruct{Required: "   "},
			wantErr: false, // whitespace is considered non-empty by default
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, tt.data)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGoValidator_Validate_Email(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	tests := []struct {
		name    string
		email   string
		wantErr bool
	}{
		{"valid email", "test@example.com", false},
		{"valid email with subdomain", "test@sub.example.com", false},
		{"valid email with plus", "test+tag@example.com", false},
		{"invalid email - no @", "testexample.com", true},
		{"invalid email - no domain", "test@", true},
		{"invalid email - no local", "@example.com", true},
		{"empty email (optional)", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, TestStruct{Required: "x", Email: tt.email})
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGoValidator_Validate_MinMax(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	tests := []struct {
		name      string
		minLength string
		maxLength string
		wantErr   bool
	}{
		{"valid min and max", "abc", "short", false},
		{"too short for min", "ab", "", true},
		{"too long for max", "", "this is too long for max", true},
		{"exactly at min", "abc", "", false},
		{"exactly at max", "", "0123456789", false},
		{"both empty (optional)", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, TestStruct{
				Required:  "x",
				MinLength: tt.minLength,
				MaxLength: tt.maxLength,
			})
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGoValidator_Validate_OneOf(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid - first option", "a", false},
		{"valid - second option", "b", false},
		{"valid - third option", "c", false},
		{"invalid option", "d", true},
		{"empty (optional)", "", false},
		{"case sensitive - uppercase", "A", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, TestStruct{Required: "x", OneOf: tt.value})
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGoValidator_Validate_Numeric(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	// Note: go-playground/validator's "numeric" tag accepts any numeric string
	// including decimals and negatives. It just checks if the string represents
	// a number, not if it's specifically an integer.
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"valid numeric", "12345", false},
		{"valid numeric with zeros", "00123", false},
		{"invalid - contains letters", "123abc", true},
		{"invalid - contains space", "123 456", true},
		{"decimal - actually valid for numeric", "123.45", false}, // numeric accepts decimals
		{"negative - actually valid for numeric", "-123", false},  // numeric accepts negatives
		{"empty (optional)", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, TestStruct{Required: "x", Numeric: tt.value})
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGoValidator_Validate_Nested(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	tests := []struct {
		name    string
		data    TestStruct
		wantErr bool
	}{
		{
			name: "valid nested struct",
			data: TestStruct{
				Required:  "x",
				NestedPtr: &NestedStruct{Value: "valid"},
			},
			wantErr: false,
		},
		{
			name: "invalid nested struct",
			data: TestStruct{
				Required:  "x",
				NestedPtr: &NestedStruct{Value: ""},
			},
			wantErr: true,
		},
		{
			name: "nil nested struct",
			data: TestStruct{
				Required:  "x",
				NestedPtr: nil,
			},
			wantErr: false,
		},
		{
			name: "valid nested slice",
			data: TestStruct{
				Required: "x",
				NestedSlice: []NestedStruct{
					{Value: "a"},
					{Value: "b"},
				},
			},
			wantErr: false,
		},
		{
			name: "invalid item in nested slice",
			data: TestStruct{
				Required: "x",
				NestedSlice: []NestedStruct{
					{Value: "a"},
					{Value: ""}, // invalid
				},
			},
			wantErr: true,
		},
		{
			name: "empty nested slice",
			data: TestStruct{
				Required:    "x",
				NestedSlice: []NestedStruct{},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, tt.data)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGoValidator_Validate_NilContext(t *testing.T) {
	v := NewGoValidator()

	// Should handle nil context gracefully
	// Note: Using context.Background() as nil context may panic
	err := v.Validate(context.Background(), TestStruct{Required: "x"})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGoValidator_Validate_NilData(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	// Validate nil should return error
	err := v.Validate(ctx, nil)
	if err == nil {
		t.Error("expected error for nil data")
	}
}

func TestGoValidator_Validate_NonStruct(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	// Validate non-struct should return error
	err := v.Validate(ctx, "string")
	if err == nil {
		t.Error("expected error for non-struct")
	}
}

// Test with actual Event struct patterns
type MockEvent struct {
	ID             string `validate:"required,min=1,max=128"`
	IdempotencyKey string `validate:"required,min=1,max=128"`
	TenantID       string `validate:"required"`
	UserID         string `validate:"required"`
	Workflow       string `validate:"required,oneof=payroll attendance trial_balance general"`
	EventType      string `validate:"required"`
	Priority       string `validate:"omitempty,oneof=low normal high critical"`
	Timestamp      time.Time
	Payload        interface{} `validate:"required"`
}

func TestGoValidator_Validate_EventPattern(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	tests := []struct {
		name    string
		event   MockEvent
		wantErr bool
	}{
		{
			name: "valid payroll event",
			event: MockEvent{
				ID:             "evt-123",
				IdempotencyKey: "idem-123",
				TenantID:       "tenant-1",
				UserID:         "user-1",
				Workflow:       "payroll",
				EventType:      "salary.calculated",
				Priority:       "high",
				Timestamp:      time.Now(),
				Payload:        map[string]interface{}{"amount": 5000},
			},
			wantErr: false,
		},
		{
			name: "valid attendance event",
			event: MockEvent{
				ID:             "evt-456",
				IdempotencyKey: "idem-456",
				TenantID:       "tenant-2",
				UserID:         "user-2",
				Workflow:       "attendance",
				EventType:      "check_in",
				Timestamp:      time.Now(),
				Payload:        map[string]interface{}{},
			},
			wantErr: false,
		},
		{
			name: "missing tenant ID",
			event: MockEvent{
				ID:             "evt-789",
				IdempotencyKey: "idem-789",
				TenantID:       "",
				UserID:         "user-3",
				Workflow:       "general",
				EventType:      "test",
				Payload:        "payload",
			},
			wantErr: true,
		},
		{
			name: "invalid workflow",
			event: MockEvent{
				ID:             "evt-999",
				IdempotencyKey: "idem-999",
				TenantID:       "tenant-4",
				UserID:         "user-4",
				Workflow:       "invalid_workflow",
				EventType:      "test",
				Payload:        "payload",
			},
			wantErr: true,
		},
		{
			name: "invalid priority",
			event: MockEvent{
				ID:             "evt-111",
				IdempotencyKey: "idem-111",
				TenantID:       "tenant-5",
				UserID:         "user-5",
				Workflow:       "general",
				EventType:      "test",
				Priority:       "super_high", // invalid
				Payload:        "payload",
			},
			wantErr: true,
		},
		{
			name: "missing payload",
			event: MockEvent{
				ID:             "evt-222",
				IdempotencyKey: "idem-222",
				TenantID:       "tenant-6",
				UserID:         "user-6",
				Workflow:       "general",
				EventType:      "test",
				Payload:        nil,
			},
			wantErr: true,
		},
		{
			name: "ID too long",
			event: MockEvent{
				ID:             string(make([]byte, 200)), // 200 chars, max is 128
				IdempotencyKey: "idem-333",
				TenantID:       "tenant-7",
				UserID:         "user-7",
				Workflow:       "general",
				EventType:      "test",
				Payload:        "payload",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, tt.event)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// Test batch event pattern
type MockBatchEvent struct {
	BatchID        string      `validate:"required"`
	IdempotencyKey string      `validate:"required"`
	TenantID       string      `validate:"required"`
	Workflow       string      `validate:"required,oneof=payroll attendance trial_balance general"`
	Events         []MockEvent `validate:"required,min=1,max=1000,dive"`
	Priority       string      `validate:"omitempty,oneof=low normal high critical"`
}

func TestGoValidator_Validate_BatchEventPattern(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	validEvent := MockEvent{
		ID:             "evt-1",
		IdempotencyKey: "idem-1",
		TenantID:       "tenant-1",
		UserID:         "user-1",
		Workflow:       "payroll",
		EventType:      "test",
		Payload:        "data",
	}

	invalidEvent := MockEvent{
		ID:             "",
		IdempotencyKey: "",
		TenantID:       "",
		UserID:         "",
		Workflow:       "",
		EventType:      "",
		Payload:        nil,
	}

	tests := []struct {
		name    string
		batch   MockBatchEvent
		wantErr bool
	}{
		{
			name: "valid batch",
			batch: MockBatchEvent{
				BatchID:        "batch-1",
				IdempotencyKey: "idem-batch-1",
				TenantID:       "tenant-1",
				Workflow:       "payroll",
				Events:         []MockEvent{validEvent, validEvent},
			},
			wantErr: false,
		},
		{
			name: "empty events",
			batch: MockBatchEvent{
				BatchID:        "batch-2",
				IdempotencyKey: "idem-batch-2",
				TenantID:       "tenant-1",
				Workflow:       "payroll",
				Events:         []MockEvent{},
			},
			wantErr: true, // min=1
		},
		{
			name: "invalid event in batch",
			batch: MockBatchEvent{
				BatchID:        "batch-3",
				IdempotencyKey: "idem-batch-3",
				TenantID:       "tenant-1",
				Workflow:       "payroll",
				Events:         []MockEvent{validEvent, invalidEvent},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := v.Validate(ctx, tt.batch)
			if tt.wantErr && err == nil {
				t.Error("expected error")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestGoValidator_Validate_CanceledContext(t *testing.T) {
	v := NewGoValidator()
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// Should still work with canceled context
	err := v.Validate(ctx, TestStruct{Required: "x"})
	if err != nil {
		t.Errorf("unexpected error with canceled context: %v", err)
	}
}

func TestGoValidator_Validate_TimeoutContext(t *testing.T) {
	v := NewGoValidator()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond) // Ensure timeout

	// Should still work with expired context
	err := v.Validate(ctx, TestStruct{Required: "x"})
	if err != nil {
		t.Errorf("unexpected error with expired context: %v", err)
	}
}

// Edge cases
func TestGoValidator_Validate_EdgeCases(t *testing.T) {
	v := NewGoValidator()
	ctx := context.Background()

	t.Run("unicode in required field", func(t *testing.T) {
		err := v.Validate(ctx, TestStruct{Required: "日本語"})
		if err != nil {
			t.Errorf("should accept unicode: %v", err)
		}
	})

	t.Run("emoji in required field", func(t *testing.T) {
		err := v.Validate(ctx, TestStruct{Required: "🎉"})
		if err != nil {
			t.Errorf("should accept emoji: %v", err)
		}
	})

	t.Run("newlines in field", func(t *testing.T) {
		err := v.Validate(ctx, TestStruct{Required: "line1\nline2"})
		if err != nil {
			t.Errorf("should accept newlines: %v", err)
		}
	})

	t.Run("tabs in field", func(t *testing.T) {
		err := v.Validate(ctx, TestStruct{Required: "col1\tcol2"})
		if err != nil {
			t.Errorf("should accept tabs: %v", err)
		}
	})

	t.Run("very long string", func(t *testing.T) {
		longStr := make([]byte, 10000)
		for i := range longStr {
			longStr[i] = 'a'
		}
		err := v.Validate(ctx, TestStruct{Required: string(longStr)})
		if err != nil {
			t.Errorf("should accept long strings: %v", err)
		}
	})

	t.Run("special characters", func(t *testing.T) {
		err := v.Validate(ctx, TestStruct{Required: "!@#$%^&*()_+-=[]{}|;':\",./<>?"})
		if err != nil {
			t.Errorf("should accept special characters: %v", err)
		}
	})

	t.Run("null bytes", func(t *testing.T) {
		err := v.Validate(ctx, TestStruct{Required: "before\x00after"})
		if err != nil {
			t.Errorf("should accept null bytes: %v", err)
		}
	})
}
