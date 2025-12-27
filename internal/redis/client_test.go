package redis

import (
	"context"
	"testing"
	"time"
)

// Note: These tests require a running Redis instance.
// For unit tests without Redis, you would use mocks or skip integration tests.

func TestConfig_Defaults(t *testing.T) {
	cfg := Config{
		Addr: "localhost:6379",
	}

	if cfg.Addr != "localhost:6379" {
		t.Errorf("expected addr localhost:6379, got %s", cfg.Addr)
	}
	if cfg.DB != 0 {
		t.Errorf("expected DB 0, got %d", cfg.DB)
	}
	if cfg.PoolSize != 0 {
		t.Errorf("expected default pool size 0, got %d", cfg.PoolSize)
	}
}

func TestConfig_WithPrefix(t *testing.T) {
	cfg := Config{
		Addr:   "localhost:6379",
		Prefix: "flashpipe",
	}

	if cfg.Prefix != "flashpipe" {
		t.Errorf("expected prefix flashpipe, got %s", cfg.Prefix)
	}
}

func TestClient_KeyPrefixing(t *testing.T) {
	// Test the key prefixing logic without connecting to Redis
	tests := []struct {
		prefix   string
		key      string
		expected string
	}{
		{"", "mykey", "mykey"},
		{"prefix", "mykey", "prefix:mykey"},
		{"app:env", "mykey", "app:env:mykey"},
		{"", "", ""},
		{"prefix", "", "prefix:"},
	}

	for _, tt := range tests {
		t.Run(tt.prefix+"/"+tt.key, func(t *testing.T) {
			c := &Client{prefix: tt.prefix}
			result := c.key(tt.key)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestRateLimitResult_Fields(t *testing.T) {
	now := time.Now()
	result := RateLimitResult{
		Allowed:   true,
		Remaining: 10,
		ResetAt:   now,
	}

	if !result.Allowed {
		t.Error("expected Allowed to be true")
	}
	if result.Remaining != 10 {
		t.Errorf("expected Remaining 10, got %d", result.Remaining)
	}
	if result.ResetAt != now {
		t.Error("expected ResetAt to match")
	}
}

func TestLock_Fields(t *testing.T) {
	lock := &Lock{
		key:   "test-lock",
		value: "owner-123",
		ttl:   30 * time.Second,
	}

	if lock.key != "test-lock" {
		t.Errorf("expected key test-lock, got %s", lock.key)
	}
	if lock.value != "owner-123" {
		t.Errorf("expected value owner-123, got %s", lock.value)
	}
	if lock.ttl != 30*time.Second {
		t.Errorf("expected ttl 30s, got %v", lock.ttl)
	}
}

func TestErrors(t *testing.T) {
	if ErrLockNotAcquired.Error() != "lock not acquired" {
		t.Errorf("unexpected error message: %s", ErrLockNotAcquired.Error())
	}
	if ErrLockNotHeld.Error() != "lock not held" {
		t.Errorf("unexpected error message: %s", ErrLockNotHeld.Error())
	}
}

// Integration tests - skip if no Redis available
func skipIfNoRedis(t *testing.T) *Client {
	t.Helper()
	client, err := NewClient(Config{
		Addr:         "localhost:6379",
		PoolSize:     5,
		MinIdleConns: 1,
		MaxRetries:   1,
		DialTimeout:  1 * time.Second,
		Prefix:       "test",
	})
	if err != nil {
		t.Skipf("Redis not available: %v", err)
	}
	return client
}

func TestClient_Integration_Ping(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Errorf("ping failed: %v", err)
	}
}

func TestClient_Integration_SetGet(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	key := "test:setget:" + time.Now().Format(time.RFC3339Nano)

	// Set
	if err := client.Set(ctx, key, "value123", 10*time.Second); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	// Get
	val, err := client.Get(ctx, key)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if val != "value123" {
		t.Errorf("expected value123, got %s", val)
	}

	// Cleanup
	client.Delete(ctx, key)
}

func TestClient_Integration_IdempotencyKey(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	key := "test:idempotency:" + time.Now().Format(time.RFC3339Nano)

	// First set should succeed
	isNew, err := client.SetIdempotencyKey(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("set idempotency key failed: %v", err)
	}
	if !isNew {
		t.Error("expected isNew to be true")
	}

	// Second set should fail (key exists)
	isNew, err = client.SetIdempotencyKey(ctx, key, 10*time.Second)
	if err != nil {
		t.Fatalf("set idempotency key failed: %v", err)
	}
	if isNew {
		t.Error("expected isNew to be false for duplicate")
	}

	// Check should return true
	exists, err := client.CheckIdempotencyKey(ctx, key)
	if err != nil {
		t.Fatalf("check idempotency key failed: %v", err)
	}
	if !exists {
		t.Error("expected key to exist")
	}
}

func TestClient_Integration_Lock(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	lockName := "test:lock:" + time.Now().Format(time.RFC3339Nano)

	// Acquire lock
	lock, err := client.AcquireLock(ctx, lockName, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire lock failed: %v", err)
	}

	// Try to acquire same lock again - should fail
	_, err = client.AcquireLock(ctx, lockName, 10*time.Second)
	if err != ErrLockNotAcquired {
		t.Errorf("expected ErrLockNotAcquired, got %v", err)
	}

	// Extend lock
	if err := lock.Extend(ctx, 20*time.Second); err != nil {
		t.Errorf("extend lock failed: %v", err)
	}

	// Release lock
	if err := lock.Release(ctx); err != nil {
		t.Errorf("release lock failed: %v", err)
	}

	// Release again should fail
	if err := lock.Release(ctx); err != ErrLockNotHeld {
		t.Errorf("expected ErrLockNotHeld, got %v", err)
	}

	// Now should be able to acquire again
	lock2, err := client.AcquireLock(ctx, lockName, 10*time.Second)
	if err != nil {
		t.Fatalf("acquire lock after release failed: %v", err)
	}
	lock2.Release(ctx)
}

func TestClient_Integration_RateLimit(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	key := "test:ratelimit:" + time.Now().Format(time.RFC3339Nano)

	// First 5 requests should be allowed
	for i := 0; i < 5; i++ {
		result, err := client.CheckRateLimit(ctx, key, 5, 10*time.Second)
		if err != nil {
			t.Fatalf("rate limit check failed: %v", err)
		}
		if !result.Allowed {
			t.Errorf("request %d should be allowed", i)
		}
	}

	// 6th request should be denied
	result, err := client.CheckRateLimit(ctx, key, 5, 10*time.Second)
	if err != nil {
		t.Fatalf("rate limit check failed: %v", err)
	}
	if result.Allowed {
		t.Error("6th request should be denied")
	}
}

func TestClient_Integration_Delete(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	keys := []string{
		"test:delete1:" + time.Now().Format(time.RFC3339Nano),
		"test:delete2:" + time.Now().Format(time.RFC3339Nano),
	}

	// Set values
	for _, key := range keys {
		client.Set(ctx, key, "value", 10*time.Second)
	}

	// Delete
	if err := client.Delete(ctx, keys...); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Verify deleted
	for _, key := range keys {
		_, err := client.Get(ctx, key)
		if err == nil {
			t.Errorf("key %s should be deleted", key)
		}
	}
}

func TestClient_Integration_Stats(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	stats := client.Stats()
	if stats == nil {
		t.Error("expected stats to be returned")
	}
}

func TestClient_Integration_Close(t *testing.T) {
	client := skipIfNoRedis(t)

	// Close should succeed
	if err := client.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}

	// Ping after close should fail
	ctx := context.Background()
	if err := client.Ping(ctx); err == nil {
		t.Error("ping after close should fail")
	}
}

// Edge case tests
func TestClient_Integration_EdgeCases(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()

	t.Run("empty key", func(t *testing.T) {
		err := client.Set(ctx, "", "value", 10*time.Second)
		// Redis accepts empty keys
		if err != nil {
			t.Logf("empty key error (expected behavior may vary): %v", err)
		}
	})

	t.Run("very long key", func(t *testing.T) {
		longKey := make([]byte, 1000)
		for i := range longKey {
			longKey[i] = 'k'
		}
		err := client.Set(ctx, "longkey:"+string(longKey), "value", 10*time.Second)
		if err != nil {
			t.Errorf("should handle long keys: %v", err)
		}
		client.Delete(ctx, "longkey:"+string(longKey))
	})

	t.Run("unicode key", func(t *testing.T) {
		key := "test:unicode:日本語:🎉"
		err := client.Set(ctx, key, "value", 10*time.Second)
		if err != nil {
			t.Errorf("should handle unicode keys: %v", err)
		}
		client.Delete(ctx, key)
	})

	t.Run("binary value", func(t *testing.T) {
		key := "test:binary:" + time.Now().Format(time.RFC3339Nano)
		binary := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
		err := client.Set(ctx, key, string(binary), 10*time.Second)
		if err != nil {
			t.Errorf("should handle binary values: %v", err)
		}
		client.Delete(ctx, key)
	})

	t.Run("zero TTL", func(t *testing.T) {
		key := "test:zerottl:" + time.Now().Format(time.RFC3339Nano)
		err := client.Set(ctx, key, "value", 0) // 0 = no expiration
		if err != nil {
			t.Errorf("should handle zero TTL: %v", err)
		}
		client.Delete(ctx, key)
	})

	t.Run("negative TTL", func(t *testing.T) {
		key := "test:negttl:" + time.Now().Format(time.RFC3339Nano)
		err := client.Set(ctx, key, "value", -1*time.Second)
		// Behavior with negative TTL may vary
		if err != nil {
			t.Logf("negative TTL error (may be expected): %v", err)
		}
	})

	t.Run("canceled context", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := client.Ping(canceledCtx)
		if err == nil {
			t.Error("should fail with canceled context")
		}
	})

	t.Run("timeout context", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
		defer cancel()
		time.Sleep(1 * time.Millisecond)

		err := client.Ping(timeoutCtx)
		if err == nil {
			t.Error("should fail with expired context")
		}
	})
}

func TestClient_Integration_ConcurrentLocking(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	lockName := "test:concurrent:" + time.Now().Format(time.RFC3339Nano)

	acquired := make(chan bool, 10)

	// Try to acquire the same lock from multiple goroutines
	for i := 0; i < 10; i++ {
		go func() {
			lock, err := client.AcquireLock(ctx, lockName, 5*time.Second)
			if err == nil {
				acquired <- true
				time.Sleep(100 * time.Millisecond)
				lock.Release(ctx)
			} else {
				acquired <- false
			}
		}()
	}

	// Collect results
	successCount := 0
	for i := 0; i < 10; i++ {
		if <-acquired {
			successCount++
		}
	}

	// Only one should have acquired initially (others may acquire after release)
	if successCount == 0 {
		t.Error("at least one goroutine should acquire the lock")
	}
	t.Logf("Acquired count: %d (expected at least 1)", successCount)
}

func TestClient_Integration_IdempotencyExpiration(t *testing.T) {
	client := skipIfNoRedis(t)
	defer client.Close()

	ctx := context.Background()
	key := "test:expiry:" + time.Now().Format(time.RFC3339Nano)

	// Set with very short TTL
	_, err := client.SetIdempotencyKey(ctx, key, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("set failed: %v", err)
	}

	// Should exist
	exists, _ := client.CheckIdempotencyKey(ctx, key)
	if !exists {
		t.Error("key should exist immediately after set")
	}

	// Wait for expiration
	time.Sleep(200 * time.Millisecond)

	// Should not exist
	exists, _ = client.CheckIdempotencyKey(ctx, key)
	if exists {
		t.Error("key should have expired")
	}
}

func TestNewClient_ConnectionError(t *testing.T) {
	// Try to connect to non-existent Redis
	_, err := NewClient(Config{
		Addr:        "localhost:59999", // Unlikely to have Redis here
		DialTimeout: 100 * time.Millisecond,
	})
	if err == nil {
		t.Error("expected connection error")
	}
}

func TestNewClient_InvalidAddress(t *testing.T) {
	_, err := NewClient(Config{
		Addr:        "invalid:address:format:123",
		DialTimeout: 100 * time.Millisecond,
	})
	if err == nil {
		t.Error("expected error for invalid address")
	}
}
