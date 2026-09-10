package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// bucketScript — атомарный token bucket.
// KEYS[1] — ключ; ARGV: capacity, refill/сек, now_ms, cost.
var bucketScript = redis.NewScript(`
local capacity = tonumber(ARGV[1])
local refill   = tonumber(ARGV[2])
local now      = tonumber(ARGV[3])
local cost     = tonumber(ARGV[4])
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
local ts     = tonumber(redis.call('HGET', KEYS[1], 'ts'))
if tokens == nil then
    tokens, ts = capacity, now
end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(capacity, tokens + elapsed * refill)
local allowed = 0
if tokens >= cost then
    tokens = tokens - cost
    allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 3600000)
return allowed
`)

type Limiter struct {
	rdb      *redis.Client
	capacity int
	refill   float64
}

func New(rdb *redis.Client, capacity int, refillPerSec float64) *Limiter {
	return &Limiter{rdb: rdb, capacity: capacity, refill: refillPerSec}
}

// Allow расходует один токен. false = лимит исчерпан.
func (l *Limiter) Allow(ctx context.Context, key string) (bool, error) {
	res, err := bucketScript.Run(ctx, l.rdb, []string{"rl:" + key},
		l.capacity, l.refill, time.Now().UnixMilli(), 1).Int()
	if err != nil {
		return false, fmt.Errorf("redis: %w", err)
	}
	return res == 1, nil
}
