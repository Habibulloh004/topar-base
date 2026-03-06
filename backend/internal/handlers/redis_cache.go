package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
)

const (
	cacheNamespaceEksmoProducts           = "eksmoProducts"
	cacheNamespaceEksmoProductsDuplicates = "eksmoProductsDuplicates"
	cacheNamespaceEksmoProductsMeta       = "eksmoProductsMeta"
	cacheNamespaceMainProducts            = "mainProducts"
	cacheKeyPrefix                        = "topar:api-cache:"
)

func (h *EksmoProductHandler) tryServeCachedJSON(c *fiber.Ctx, namespace string) (bool, error) {
	if h.redisClient == nil || strings.ToUpper(c.Method()) != fiber.MethodGet {
		return false, nil
	}

	key := buildRedisCacheKey(namespace, c)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	data, err := h.redisClient.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, nil
	}

	c.Set("X-Cache", "HIT")
	return true, c.Status(fiber.StatusOK).Type("json").Send(data)
}

func (h *EksmoProductHandler) respondJSONWithCache(c *fiber.Ctx, namespace string, payload interface{}) error {
	if h.redisClient == nil || strings.ToUpper(c.Method()) != fiber.MethodGet {
		return c.JSON(payload)
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return c.JSON(payload)
	}

	ttl := h.redisCacheTTL
	if ttl < 0 {
		ttl = 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_ = h.redisClient.Set(ctx, buildRedisCacheKey(namespace, c), encoded, ttl).Err()
	c.Set("X-Cache", "MISS")
	return c.Status(fiber.StatusOK).Type("json").Send(encoded)
}

func (h *EksmoProductHandler) invalidateProductCaches() {
	invalidateProductCachesByRedis(h.redisClient)
}

func (h *EksmoProductHandler) deleteRedisPattern(pattern string) {
	deleteRedisPatternByRedis(h.redisClient, pattern)
}

func invalidateProductCachesByRedis(redisClient *redis.Client) {
	if redisClient == nil {
		return
	}
	for _, namespace := range []string{
		cacheNamespaceEksmoProducts,
		cacheNamespaceEksmoProductsDuplicates,
		cacheNamespaceEksmoProductsMeta,
		cacheNamespaceMainProducts,
	} {
		deleteRedisPatternByRedis(redisClient, cacheKeyPrefix+namespace+":*")
	}
}

func deleteRedisPatternByRedis(redisClient *redis.Client, pattern string) {
	if redisClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	keys := make([]string, 0, 512)
	iter := redisClient.Scan(ctx, 0, pattern, 500).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= 500 {
			_ = redisClient.Del(ctx, keys...).Err()
			keys = keys[:0]
		}
	}
	if len(keys) > 0 {
		_ = redisClient.Del(ctx, keys...).Err()
	}
}

func buildRedisCacheKey(namespace string, c *fiber.Ctx) string {
	queryPairs := make([]string, 0, len(c.Queries()))
	for key, value := range c.Queries() {
		queryPairs = append(queryPairs, fmt.Sprintf("%s=%s", key, value))
	}
	sort.Strings(queryPairs)

	queryPart := strings.Join(queryPairs, "&")
	if queryPart == "" {
		queryPart = "-"
	}

	return fmt.Sprintf("%s%s:%s:%s", cacheKeyPrefix, namespace, c.Path(), queryPart)
}
