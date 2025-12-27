// Package main - metrics are now defined in internal/middleware/middleware.go
// This file is kept for backwards compatibility reference only.
package main

// Prometheus metrics have been moved to internal/middleware/middleware.go
// Available metrics:
// - flashpipe_http_requests_total (counter)
// - flashpipe_http_request_duration_seconds (histogram)
// - flashpipe_http_requests_in_flight (gauge)
// - flashpipe_events_ingested_total (counter)
// - flashpipe_events_batch_size (histogram)
