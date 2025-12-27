package model

import (
	"time"

	"github.com/google/uuid"
)

// WorkflowType defines ERP workflow categories.
type WorkflowType string

const (
	WorkflowPayroll      WorkflowType = "payroll"
	WorkflowAttendance   WorkflowType = "attendance"
	WorkflowTrialBalance WorkflowType = "trial_balance"
	WorkflowGeneral      WorkflowType = "general"
)

// Priority defines event priority levels.
type Priority string

const (
	PriorityLow      Priority = "low"
	PriorityNormal   Priority = "normal"
	PriorityHigh     Priority = "high"
	PriorityCritical Priority = "critical"
)

// Event is the base event structure for all ERP events.
type Event struct {
	ID             string       `json:"id"`
	IdempotencyKey string       `json:"idempotency_key" validate:"required,min=1,max=128"`
	TenantID       string       `json:"tenant_id" validate:"required"`
	UserID         string       `json:"user_id" validate:"required"`
	Workflow       WorkflowType `json:"workflow" validate:"required,oneof=payroll attendance trial_balance general"`
	EventType      string       `json:"event_type" validate:"required"`
	Priority       Priority     `json:"priority" validate:"omitempty,oneof=low normal high critical"`
	Timestamp      time.Time    `json:"timestamp" validate:"required"`
	Payload        interface{}  `json:"payload" validate:"required"`
	Metadata       Metadata     `json:"metadata,omitempty"`
}

// Metadata contains optional event metadata.
type Metadata struct {
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	Source        string            `json:"source,omitempty"`
	Version       string            `json:"version,omitempty"`
	Tags          map[string]string `json:"tags,omitempty"`
}

// NewEvent creates a new event with generated ID.
func NewEvent(tenantID, userID string, workflow WorkflowType, eventType string, payload interface{}) *Event {
	return &Event{
		ID:             uuid.New().String(),
		IdempotencyKey: uuid.New().String(),
		TenantID:       tenantID,
		UserID:         userID,
		Workflow:       workflow,
		EventType:      eventType,
		Priority:       PriorityNormal,
		Timestamp:      time.Now().UTC(),
		Payload:        payload,
	}
}

// SetPriority sets the event priority.
func (e *Event) SetPriority(p Priority) *Event {
	e.Priority = p
	return e
}

// WithMetadata sets event metadata.
func (e *Event) WithMetadata(m Metadata) *Event {
	e.Metadata = m
	return e
}

// --- Payroll Events ---

// PayrollCalculationPayload represents payroll calculation data.
type PayrollCalculationPayload struct {
	EmployeeID    string    `json:"employee_id" validate:"required"`
	PeriodStart   time.Time `json:"period_start" validate:"required"`
	PeriodEnd     time.Time `json:"period_end" validate:"required"`
	BaseSalary    float64   `json:"base_salary" validate:"required,gt=0"`
	Allowances    float64   `json:"allowances"`
	Deductions    float64   `json:"deductions"`
	OvertimeHours float64   `json:"overtime_hours"`
	OvertimeRate  float64   `json:"overtime_rate"`
	Currency      string    `json:"currency" validate:"required,len=3"`
}

// PayrollBatchPayload represents a batch of payroll records.
type PayrollBatchPayload struct {
	BatchID     string                      `json:"batch_id" validate:"required"`
	PeriodStart time.Time                   `json:"period_start" validate:"required"`
	PeriodEnd   time.Time                   `json:"period_end" validate:"required"`
	Records     []PayrollCalculationPayload `json:"records" validate:"required,min=1,dive"`
}

// --- Attendance Events ---

// AttendanceRecordPayload represents a single attendance record.
type AttendanceRecordPayload struct {
	EmployeeID   string    `json:"employee_id" validate:"required"`
	Date         time.Time `json:"date" validate:"required"`
	CheckIn      time.Time `json:"check_in"`
	CheckOut     time.Time `json:"check_out"`
	BreakMinutes int       `json:"break_minutes"`
	Status       string    `json:"status" validate:"required,oneof=present absent late leave"`
	Location     string    `json:"location,omitempty"`
	DeviceID     string    `json:"device_id,omitempty"`
}

// AttendanceBatchPayload represents a batch of attendance records.
type AttendanceBatchPayload struct {
	BatchID string                    `json:"batch_id" validate:"required"`
	Date    time.Time                 `json:"date" validate:"required"`
	Records []AttendanceRecordPayload `json:"records" validate:"required,min=1,dive"`
}

// --- Trial Balance Events ---

// TrialBalanceEntryPayload represents a trial balance entry.
type TrialBalanceEntryPayload struct {
	AccountCode string  `json:"account_code" validate:"required"`
	AccountName string  `json:"account_name" validate:"required"`
	Debit       float64 `json:"debit" validate:"gte=0"`
	Credit      float64 `json:"credit" validate:"gte=0"`
	Currency    string  `json:"currency" validate:"required,len=3"`
}

// TrialBalanceReportPayload represents a trial balance report request.
type TrialBalanceReportPayload struct {
	ReportID    string                     `json:"report_id" validate:"required"`
	PeriodStart time.Time                  `json:"period_start" validate:"required"`
	PeriodEnd   time.Time                  `json:"period_end" validate:"required"`
	Entries     []TrialBalanceEntryPayload `json:"entries" validate:"required,min=1,dive"`
}

// --- Batch Event Wrapper ---

// BatchEvent wraps multiple events for batch processing.
type BatchEvent struct {
	BatchID        string       `json:"batch_id"`
	IdempotencyKey string       `json:"idempotency_key" validate:"required"`
	TenantID       string       `json:"tenant_id" validate:"required"`
	Workflow       WorkflowType `json:"workflow" validate:"required"`
	Events         []Event      `json:"events" validate:"required,min=1,max=1000,dive"`
	Priority       Priority     `json:"priority" validate:"omitempty,oneof=low normal high critical"`
	Timestamp      time.Time    `json:"timestamp"`
}

// NewBatchEvent creates a new batch event.
func NewBatchEvent(tenantID string, workflow WorkflowType, events []Event) *BatchEvent {
	return &BatchEvent{
		BatchID:        uuid.New().String(),
		IdempotencyKey: uuid.New().String(),
		TenantID:       tenantID,
		Workflow:       workflow,
		Events:         events,
		Priority:       PriorityNormal,
		Timestamp:      time.Now().UTC(),
	}
}

// --- Response Models ---

// IngestResponse is returned after successful event ingestion.
type IngestResponse struct {
	EventID   string `json:"event_id"`
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}

// BatchIngestResponse is returned after batch ingestion.
type BatchIngestResponse struct {
	BatchID     string   `json:"batch_id"`
	Accepted    int      `json:"accepted"`
	Rejected    int      `json:"rejected"`
	RejectedIDs []string `json:"rejected_ids,omitempty"`
	Status      string   `json:"status"`
	Timestamp   int64    `json:"timestamp"`
}

// ErrorResponse represents an error response.
type ErrorResponse struct {
	Code      string            `json:"code"`
	Message   string            `json:"message"`
	Details   map[string]string `json:"details,omitempty"`
	Timestamp int64             `json:"timestamp"`
}
