package redis

import (
	"context"

	redisclient "github.com/redis/go-redis/v9"
)

type redisClient interface {
	Ping(context.Context) error
	HGetAll(context.Context, string) (map[string]string, error)
	Scan(context.Context, uint64, string, int64) ([]string, uint64, error)
	Exists(context.Context, string) (bool, error)
	EvalInt(context.Context, string, []string, ...any) (int64, error)
	HSet(context.Context, string, map[string]string) error
	HDel(context.Context, string, ...string) (int64, error)
	Del(context.Context, ...string) (int64, error)
	Type(context.Context, string) (string, error)
	Close() error
}

type clientFactory func(NamespaceConfig) redisClient

type goRedisClient struct {
	client *redisclient.Client
}

func newRedisClient(config NamespaceConfig) redisClient {
	return &goRedisClient{client: redisclient.NewClient(&redisclient.Options{
		Addr:         config.Address,
		Username:     config.Username,
		Password:     config.Password,
		DB:           config.Database,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
		PoolSize:     config.PoolSize,
		TLSConfig:    config.tlsConfig(),
	})}
}

func (c *goRedisClient) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

func (c *goRedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.client.HGetAll(ctx, key).Result()
}

func (c *goRedisClient) Scan(
	ctx context.Context,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	return c.client.Scan(ctx, cursor, pattern, count).Result()
}

func (c *goRedisClient) Exists(ctx context.Context, key string) (bool, error) {
	count, err := c.client.Exists(ctx, key).Result()
	return count > 0, err
}

func (c *goRedisClient) EvalInt(
	ctx context.Context,
	script string,
	keys []string,
	args ...any,
) (int64, error) {
	return c.client.Eval(ctx, script, keys, args...).Int64()
}

func (c *goRedisClient) HSet(ctx context.Context, key string, fields map[string]string) error {
	return c.client.HSet(ctx, key, fields).Err()
}

func (c *goRedisClient) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.client.HDel(ctx, key, fields...).Result()
}

func (c *goRedisClient) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.client.Del(ctx, keys...).Result()
}

func (c *goRedisClient) Type(ctx context.Context, key string) (string, error) {
	return c.client.Type(ctx, key).Result()
}

func (c *goRedisClient) Close() error {
	return c.client.Close()
}

var _ redisClient = (*goRedisClient)(nil)
