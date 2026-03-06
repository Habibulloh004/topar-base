package db

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func ConnectRedis(ctx context.Context, addr string, password string, redisDB int) (*redis.Client, error) {
	opts := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       redisDB,
	}
	client := redis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}

	return client, nil
}
