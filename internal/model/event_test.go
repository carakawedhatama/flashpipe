package model

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewEvent(t *testing.T) {
	tests := []struct {
		name      string
		tenantID  string
		userID    string
		workflow  WorkflowType
		eventType string
		payload   interface{}
	}{
		{
			name:      "creates payroll event",
			tenantID:  "tenant-123",
			userID:    "user-456",
			workflow:  WorkflowPayroll,
			eventType: "salary.calculated",
			payload:   map[string]interface{}{"amount": 5000},
		},
		{
			name:      "creates attendance event",
			tenantID:  "tenant-abc",
			userID:    "user-xyz",
			workflow:  WorkflowAttendance,
			eventType: "check_in",
			payload:   map[string]interface{}{"location": "office"},
		},
		{
			name:      "creates event with empty payload",
			tenantID:  "tenant-1",
			userID:    "user-1",
			workflow:  WorkflowGeneral,
			eventType: "test",
			payload:   nil,
		},
		{
			name:      "creates event with complex payload",
			tenantID:  "tenant-complex",
			userID:    "user-complex",
			workflow:  WorkflowTrialBalance,
			eventType: "report.generated",
			payload: map[string]interface{}{
				"nested": map[string]interface{}{
					"deeply": map[string]interface{}{
						"value": 123,
					},
				},
				"array": []int{1, 2, 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := NewEvent(tt.tenantID, tt.userID, tt.workflow, tt.eventType, tt.payload)

			if evt.ID == "" {
				t.Error("expected ID to be generated")
			}
			if evt.IdempotencyKey == "" {
				t.Error("expected IdempotencyKey to be generated")
			}
			if evt.TenantID != tt.tenantID {
				t.Errorf("expected TenantID %s, got %s", tt.tenantID, evt.TenantID)
			}
			if evt.UserID != tt.userID {
				t.Errorf("expected UserID %s, got %s", tt.userID, evt.UserID)
			}
			if evt.Workflow != tt.workflow {
				t.Errorf("expected Workflow %s, got %s", tt.workflow, evt.Workflow)
			}
			if evt.EventType != tt.eventType {
				t.Errorf("expected EventType %s, got %s", tt.eventType, evt.EventType)
			}
			if evt.Priority != PriorityNormal {
				t.Errorf("expected default Priority %s, got %s", PriorityNormal, evt.Priority)
			}
			if evt.Timestamp.IsZero() {
				t.Error("expected Timestamp to be set")
			}
		})
	}
}

func TestEvent_SetPriority(t *testing.T) {
	priorities := []Priority{
		PriorityLow,
		PriorityNormal,
		PriorityHigh,
		PriorityCritical,
	}

	for _, p := range priorities {
		t.Run(string(p), func(t *testing.T) {
			evt := NewEvent("tenant", "user", WorkflowGeneral, "test", nil)
			result := evt.SetPriority(p)

			if evt.Priority != p {
				t.Errorf("expected priority %s, got %s", p, evt.Priority)
			}
			if result != evt {
				t.Error("expected SetPriority to return same event for chaining")
			}
		})
	}
}

func TestEvent_WithMetadata(t *testing.T) {
	tests := []struct {
		name     string
		metadata Metadata
	}{
		{
			name: "with correlation ID",
			metadata: Metadata{
				CorrelationID: "corr-123",
			},
		},
		{
			name: "with full metadata",
			metadata: Metadata{
				CorrelationID: "corr-456",
				CausationID:   "cause-789",
				Source:        "test-source",
				Version:       "1.0.0",
				Tags:          map[string]string{"env": "test", "region": "us-east"},
			},
		},
		{
			name:     "with empty metadata",
			metadata: Metadata{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := NewEvent("tenant", "user", WorkflowGeneral, "test", nil)
			result := evt.WithMetadata(tt.metadata)

			if evt.Metadata.CorrelationID != tt.metadata.CorrelationID {
				t.Errorf("expected CorrelationID %s, got %s", tt.metadata.CorrelationID, evt.Metadata.CorrelationID)
			}
			if result != evt {
				t.Error("expected WithMetadata to return same event for chaining")
			}
		})
	}
}

func TestEvent_JSONSerialization(t *testing.T) {
	evt := NewEvent("tenant-123", "user-456", WorkflowPayroll, "salary.calculated", PayrollCalculationPayload{
		EmployeeID:  "emp-789",
		PeriodStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:   time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC),
		BaseSalary:  5000000,
		Currency:    "IDR",
	})

	// Serialize
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("failed to marshal event: %v", err)
	}

	// Deserialize
	var decoded Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal event: %v", err)
	}

	if decoded.ID != evt.ID {
		t.Errorf("expected ID %s, got %s", evt.ID, decoded.ID)
	}
	if decoded.TenantID != evt.TenantID {
		t.Errorf("expected TenantID %s, got %s", evt.TenantID, decoded.TenantID)
	}
}

func TestNewBatchEvent(t *testing.T) {
	events := []Event{
		*NewEvent("tenant-1", "user-1", WorkflowPayroll, "test1", nil),
		*NewEvent("tenant-1", "user-2", WorkflowPayroll, "test2", nil),
	}

	batch := NewBatchEvent("tenant-1", WorkflowPayroll, events)

	if batch.BatchID == "" {
		t.Error("expected BatchID to be generated")
	}
	if batch.IdempotencyKey == "" {
		t.Error("expected IdempotencyKey to be generated")
	}
	if batch.TenantID != "tenant-1" {
		t.Errorf("expected TenantID tenant-1, got %s", batch.TenantID)
	}
	if batch.Workflow != WorkflowPayroll {
		t.Errorf("expected Workflow payroll, got %s", batch.Workflow)
	}
	if len(batch.Events) != 2 {
		t.Errorf("expected 2 events, got %d", len(batch.Events))
	}
	if batch.Priority != PriorityNormal {
		t.Errorf("expected default priority normal, got %s", batch.Priority)
	}
}

func TestWorkflowType_Values(t *testing.T) {
	tests := []struct {
		workflow WorkflowType
		expected string
	}{
		{WorkflowPayroll, "payroll"},
		{WorkflowAttendance, "attendance"},
		{WorkflowTrialBalance, "trial_balance"},
		{WorkflowGeneral, "general"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if string(tt.workflow) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.workflow)
			}
		})
	}
}

func TestPriority_Values(t *testing.T) {
	tests := []struct {
		priority Priority
		expected string
	}{
		{PriorityLow, "low"},
		{PriorityNormal, "normal"},
		{PriorityHigh, "high"},
		{PriorityCritical, "critical"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			if string(tt.priority) != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, tt.priority)
			}
		})
	}
}

func TestPayrollCalculationPayload_JSONSerialization(t *testing.T) {
	payload := PayrollCalculationPayload{
		EmployeeID:    "emp-123",
		PeriodStart:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		PeriodEnd:     time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC),
		BaseSalary:    5000000,
		Allowances:    500000,
		Deductions:    250000,
		OvertimeHours: 10,
		OvertimeRate:  50000,
		Currency:      "IDR",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded PayrollCalculationPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.EmployeeID != payload.EmployeeID {
		t.Errorf("expected EmployeeID %s, got %s", payload.EmployeeID, decoded.EmployeeID)
	}
	if decoded.BaseSalary != payload.BaseSalary {
		t.Errorf("expected BaseSalary %f, got %f", payload.BaseSalary, decoded.BaseSalary)
	}
}

func TestAttendanceRecordPayload_JSONSerialization(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	payload := AttendanceRecordPayload{
		EmployeeID:   "emp-456",
		Date:         now,
		CheckIn:      now.Add(-8 * time.Hour),
		CheckOut:     now,
		BreakMinutes: 60,
		Status:       "present",
		Location:     "HQ",
		DeviceID:     "device-001",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded AttendanceRecordPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.EmployeeID != payload.EmployeeID {
		t.Errorf("expected EmployeeID %s, got %s", payload.EmployeeID, decoded.EmployeeID)
	}
	if decoded.Status != payload.Status {
		t.Errorf("expected Status %s, got %s", payload.Status, decoded.Status)
	}
}

func TestTrialBalanceEntryPayload_JSONSerialization(t *testing.T) {
	payload := TrialBalanceEntryPayload{
		AccountCode: "1001",
		AccountName: "Cash",
		Debit:       100000,
		Credit:      0,
		Currency:    "USD",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded TrialBalanceEntryPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.AccountCode != payload.AccountCode {
		t.Errorf("expected AccountCode %s, got %s", payload.AccountCode, decoded.AccountCode)
	}
	if decoded.Debit != payload.Debit {
		t.Errorf("expected Debit %f, got %f", payload.Debit, decoded.Debit)
	}
}

func TestIngestResponse_JSONSerialization(t *testing.T) {
	resp := IngestResponse{
		EventID:   "evt-123",
		Status:    "accepted",
		Timestamp: time.Now().UnixMilli(),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded IngestResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.EventID != resp.EventID {
		t.Errorf("expected EventID %s, got %s", resp.EventID, decoded.EventID)
	}
}

func TestBatchIngestResponse_JSONSerialization(t *testing.T) {
	resp := BatchIngestResponse{
		BatchID:     "batch-123",
		Accepted:    95,
		Rejected:    5,
		RejectedIDs: []string{"evt-1", "evt-2", "evt-3", "evt-4", "evt-5"},
		Status:      "accepted",
		Timestamp:   time.Now().UnixMilli(),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded BatchIngestResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.Accepted != resp.Accepted {
		t.Errorf("expected Accepted %d, got %d", resp.Accepted, decoded.Accepted)
	}
	if len(decoded.RejectedIDs) != len(resp.RejectedIDs) {
		t.Errorf("expected %d rejected IDs, got %d", len(resp.RejectedIDs), len(decoded.RejectedIDs))
	}
}

func TestErrorResponse_JSONSerialization(t *testing.T) {
	resp := ErrorResponse{
		Code:    "VALIDATION_ERROR",
		Message: "Invalid input",
		Details: map[string]string{
			"field": "email",
			"error": "invalid format",
		},
		Timestamp: time.Now().UnixMilli(),
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded ErrorResponse
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.Code != resp.Code {
		t.Errorf("expected Code %s, got %s", resp.Code, decoded.Code)
	}
	if decoded.Details["field"] != "email" {
		t.Errorf("expected field detail 'email', got '%s'", decoded.Details["field"])
	}
}

// Edge case tests
func TestEvent_EdgeCases(t *testing.T) {
	t.Run("empty strings", func(t *testing.T) {
		evt := NewEvent("", "", WorkflowGeneral, "", nil)
		if evt.ID == "" {
			t.Error("ID should still be generated")
		}
	})

	t.Run("very long strings", func(t *testing.T) {
		longStr := make([]byte, 10000)
		for i := range longStr {
			longStr[i] = 'a'
		}
		evt := NewEvent(string(longStr), string(longStr), WorkflowGeneral, string(longStr), nil)
		if evt.TenantID != string(longStr) {
			t.Error("should handle long strings")
		}
	})

	t.Run("special characters", func(t *testing.T) {
		special := "tenant-!@#$%^&*()_+-=[]{}|;':\",./<>?"
		evt := NewEvent(special, special, WorkflowGeneral, special, nil)
		if evt.TenantID != special {
			t.Error("should handle special characters")
		}
	})

	t.Run("unicode characters", func(t *testing.T) {
		unicode := "tenant-日本語-中文-한국어-🎉"
		evt := NewEvent(unicode, unicode, WorkflowGeneral, unicode, nil)
		if evt.TenantID != unicode {
			t.Error("should handle unicode characters")
		}
	})

	t.Run("whitespace only", func(t *testing.T) {
		whitespace := "   \t\n\r  "
		evt := NewEvent(whitespace, whitespace, WorkflowGeneral, whitespace, nil)
		if evt.TenantID != whitespace {
			t.Error("should preserve whitespace")
		}
	})
}

func TestBatchEvent_EdgeCases(t *testing.T) {
	t.Run("empty events slice", func(t *testing.T) {
		batch := NewBatchEvent("tenant", WorkflowGeneral, []Event{})
		if len(batch.Events) != 0 {
			t.Error("should handle empty events")
		}
	})

	t.Run("large batch", func(t *testing.T) {
		events := make([]Event, 1000)
		for i := 0; i < 1000; i++ {
			events[i] = *NewEvent("tenant", "user", WorkflowGeneral, "test", nil)
		}
		batch := NewBatchEvent("tenant", WorkflowGeneral, events)
		if len(batch.Events) != 1000 {
			t.Errorf("expected 1000 events, got %d", len(batch.Events))
		}
	})
}

func TestPayload_EdgeCases(t *testing.T) {
	t.Run("zero values in payroll", func(t *testing.T) {
		payload := PayrollCalculationPayload{
			EmployeeID:  "emp-1",
			PeriodStart: time.Time{},
			PeriodEnd:   time.Time{},
			BaseSalary:  0,
			Currency:    "",
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("should marshal zero values: %v", err)
		}
		if len(data) == 0 {
			t.Error("should produce valid JSON")
		}
	})

	t.Run("negative values in payroll", func(t *testing.T) {
		payload := PayrollCalculationPayload{
			EmployeeID: "emp-1",
			BaseSalary: -1000, // Negative salary (edge case)
			Deductions: -500,  // Negative deductions
			Currency:   "USD",
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("should marshal negative values: %v", err)
		}

		var decoded PayrollCalculationPayload
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("should unmarshal negative values: %v", err)
		}
		if decoded.BaseSalary != -1000 {
			t.Error("should preserve negative values")
		}
	})

	t.Run("very large numbers", func(t *testing.T) {
		payload := PayrollCalculationPayload{
			EmployeeID: "emp-1",
			BaseSalary: 999999999999999,
			Currency:   "IDR",
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("should marshal large numbers: %v", err)
		}

		var decoded PayrollCalculationPayload
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("should unmarshal large numbers: %v", err)
		}
		if decoded.BaseSalary != 999999999999999 {
			t.Error("should preserve large numbers")
		}
	})
}
