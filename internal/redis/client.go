package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrLockNotAcquired = errors.New("lock not acquired")
	ErrLockNotHeld     = errors.New("lock not held")
)

// Client wraps go-redis with high-throughput optimizations.
type Client struct {
	rdb    *redis.Client
	prefix string
}

// Config holds Redis client configuration.
type Config struct {
	Addr            string
	Password        string
	DB              int
	PoolSize        int
	MinIdleConns    int
	MaxRetries      int
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolTimeout     time.Duration
	ConnMaxIdleTime time.Duration
	Prefix          string
}

// NewClient creates a new Redis client with connection pooling optimized for high throughput.
func NewClient(cfg Config) (*Client, error) {
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 3 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 3 * time.Second
	}
	if cfg.PoolTimeout == 0 {
		cfg.PoolTimeout = 4 * time.Second
	}
	if cfg.ConnMaxIdleTime == 0 {
		cfg.ConnMaxIdleTime = 5 * time.Minute
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:            cfg.Addr,
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		MinIdleConns:    cfg.MinIdleConns,
		MaxRetries:      cfg.MaxRetries,
		DialTimeout:     cfg.DialTimeout,
		ReadTimeout:     cfg.ReadTimeout,
		WriteTimeout:    cfg.WriteTimeout,
		PoolTimeout:     cfg.PoolTimeout,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
	})

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &Client{
		rdb:    rdb,
		prefix: cfg.Prefix,
	}, nil
}

// Close closes the Redis client.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Ping verifies the connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// key prepends the configured prefix to a key.
func (c *Client) key(k string) string {
	if c.prefix == "" {
		return k
	}
	return c.prefix + ":" + k
}

// --- Idempotency Operations ---

// SetIdempotencyKey sets an idempotency key with expiration.
// Returns true if the key was set (first time), false if it already exists.
func (c *Client) SetIdempotencyKey(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	result, err := c.rdb.SetNX(ctx, c.key("idempotency:"+key), "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("set idempotency key: %w", err)
	}
	return result, nil
}

// CheckIdempotencyKey checks if an idempotency key exists.
func (c *Client) CheckIdempotencyKey(ctx context.Context, key string) (bool, error) {
	result, err := c.rdb.Exists(ctx, c.key("idempotency:"+key)).Result()
	if err != nil {
		return false, fmt.Errorf("check idempotency key: %w", err)
	}
	return result > 0, nil
}

// --- Distributed Locking ---

// Lock represents a distributed lock.
type Lock struct {
	client *Client
	key    string
	value  string
	ttl    time.Duration
}

// AcquireLock attempts to acquire a distributed lock.
// Useful for ERP workflows like payroll processing where only one process should run.
func (c *Client) AcquireLock(ctx context.Context, name string, ttl time.Duration) (*Lock, error) {
	key := c.key("lock:" + name)
	value := fmt.Sprintf("%d", time.Now().UnixNano())

	acquired, err := c.rdb.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire lock: %w", err)
	}

	if !acquired {
		return nil, ErrLockNotAcquired
	}

	return &Lock{
		client: c,
		key:    key,
		value:  value,
		ttl:    ttl,
	}, nil
}

// Release releases the lock if still held.
func (l *Lock) Release(ctx context.Context) error {
	// Lua script to release lock only if we still hold it
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, l.client.rdb, []string{l.key}, l.value).Int()
	if err != nil {
		return fmt.Errorf("release lock: %w", err)
	}

	if result == 0 {
		return ErrLockNotHeld
	}

	return nil
}

// Extend extends the lock TTL if still held.
func (l *Lock) Extend(ctx context.Context, ttl time.Duration) error {
	script := redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)

	result, err := script.Run(ctx, l.client.rdb, []string{l.key}, l.value, int(ttl.Milliseconds())).Int()
	if err != nil {
		return fmt.Errorf("extend lock: %w", err)
	}

	if result == 0 {
		return ErrLockNotHeld
	}

	return nil
}

// --- Rate Limiting ---

// RateLimitResult contains the result of a rate limit check.
type RateLimitResult struct {
	Allowed   bool
	Remaining int64
	ResetAt   time.Time
}

// CheckRateLimit implements sliding window rate limiting.
// key: unique identifier (e.g., tenant_id:workflow)
// limit: max requests allowed
// window: time window for the limit
func (c *Client) CheckRateLimit(ctx context.Context, key string, limit int64, window time.Duration) (*RateLimitResult, error) {
	now := time.Now()
	windowStart := now.Add(-window).UnixMilli()
	nowMs := now.UnixMilli()
	resetAt := now.Add(window)

	rateKey := c.key("ratelimit:" + key)

	// Lua script for atomic sliding window rate limiting
	script := redis.NewScript(`
		local key = KEYS[1]
		local window_start = tonumber(ARGV[1])
		local now = tonumber(ARGV[2])
		local limit = tonumber(ARGV[3])
		local window_ms = tonumber(ARGV[4])

		-- Remove old entries outside the window
		redis.call("ZREMRANGEBYSCORE", key, "-inf", window_start)

		-- Count current entries
		local current = redis.call("ZCARD", key)

		if current < limit then
			-- Add new entry
			redis.call("ZADD", key, now, now)
			redis.call("PEXPIRE", key, window_ms)
			return {1, limit - current - 1}
		else
			return {0, 0}
		end
	`)

	result, err := script.Run(ctx, c.rdb, []string{rateKey}, windowStart, nowMs, limit, int(window.Milliseconds())).Slice()
	if err != nil {
		return nil, fmt.Errorf("rate limit check: %w", err)
	}

	allowed := result[0].(int64) == 1
	remaining := result[1].(int64)

	return &RateLimitResult{
		Allowed:   allowed,
		Remaining: remaining,
		ResetAt:   resetAt,
	}, nil
}

// --- Batch Operations ---

// Pipeline returns a pipeline for batch operations.
func (c *Client) Pipeline() redis.Pipeliner {
	return c.rdb.Pipeline()
}

// --- Pub/Sub for Event Distribution ---

// Publish publishes a message to a channel.
func (c *Client) Publish(ctx context.Context, channel string, message interface{}) error {
	return c.rdb.Publish(ctx, c.key(channel), message).Err()
}

// Subscribe subscribes to channels.
func (c *Client) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	prefixedChannels := make([]string, len(channels))
	for i, ch := range channels {
		prefixedChannels[i] = c.key(ch)
	}
	return c.rdb.Subscribe(ctx, prefixedChannels...)
}

// --- Cache Operations ---

// Get retrieves a value from cache.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.rdb.Get(ctx, c.key(key)).Result()
}

// Set stores a value in cache with optional expiration.
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.rdb.Set(ctx, c.key(key), value, expiration).Err()
}

// Delete removes a key from cache.
func (c *Client) Delete(ctx context.Context, keys ...string) error {
	prefixedKeys := make([]string, len(keys))
	for i, k := range keys {
		prefixedKeys[i] = c.key(k)
	}
	return c.rdb.Del(ctx, prefixedKeys...).Err()
}

// --- Stats ---

// Stats returns pool statistics.
func (c *Client) Stats() *redis.PoolStats {
	return c.rdb.PoolStats()
}
