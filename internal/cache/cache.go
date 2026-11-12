// Package cache is an in-process Redis-protocol cache.
// The server is miniredis. A remote Redis is not contacted.
package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/JatinF/golang-distributed-systems-performance-lab/internal/reason"
)

// Cache talks to an in-process Redis-protocol server.
type Cache struct {
	mu     sync.Mutex
	server *miniredis.Miniredis
	client *redis.Client
}

// Start listens on 127.0.0.1 with an ephemeral port.
func Start() (*Cache, error) {
	server, err := miniredis.Run()
	if err != nil {
		return nil, err
	}
	return &Cache{server: server, client: newClient(server.Addr())}, nil
}

func newClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  200 * time.Millisecond,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 200 * time.Millisecond,
		MaxRetries:   0,
	})
}

func (c *Cache) clientNow() *redis.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client
}

// Get returns the value and a cache reason. A closed server is cache_unavailable.
// The cache does not answer with a stale value after the server is gone.
func (c *Cache) Get(ctx context.Context, key string) (string, string) {
	client := c.clientNow()
	if client == nil {
		return "", reason.CacheUnavailable
	}
	val, err := client.Get(ctx, key).Result()
	if err == nil {
		return val, reason.CacheHit
	}
	if errors.Is(err, redis.Nil) {
		return "", reason.CacheMiss
	}
	return "", reason.CacheUnavailable
}

// Set stores a value with no expiry.
func (c *Cache) Set(ctx context.Context, key, value string) error {
	client := c.clientNow()
	if client == nil {
		return errors.New(reason.CacheUnavailable)
	}
	return client.Set(ctx, key, value, 0).Err()
}

// Invalidate deletes a key.
func (c *Cache) Invalidate(ctx context.Context, key string) string {
	client := c.clientNow()
	if client == nil {
		return reason.CacheUnavailable
	}
	if err := client.Del(ctx, key).Err(); err != nil {
		return reason.CacheUnavailable
	}
	return reason.CacheInvalidated
}

// Stop closes the in-process server and the client.
func (c *Cache) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil {
		_ = c.client.Close()
		c.client = nil
	}
	if c.server != nil {
		c.server.Close()
		c.server = nil
	}
}

// Recover starts a new in-process server and points the client at it.
// The new server is empty, so the next read is a miss.
func (c *Cache) Recover() (string, error) {
	server, err := miniredis.Run()
	if err != nil {
		return reason.CacheUnavailable, err
	}
	client := newClient(server.Addr())
	c.mu.Lock()
	if c.client != nil {
		_ = c.client.Close()
	}
	if c.server != nil {
		c.server.Close()
	}
	c.server = server
	c.client = client
	c.mu.Unlock()
	return reason.CacheRecovered, nil
}

// Close releases the server. It is safe to call more than once.
func (c *Cache) Close() { c.Stop() }
