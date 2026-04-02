package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
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

	redisCacheLookupTimeout = 200 * time.Millisecond
	redisCacheWriteTimeout  = 200 * time.Millisecond
	redisCacheScanTimeout   = 3 * time.Second
	redisCacheDeleteTimeout = 3 * time.Second
	redisCacheScanCount     = int64(1000)
	redisCacheDeleteBatch   = 500
	redisCacheDeletePasses  = 5
)

func (h *EksmoProductHandler) tryServeCachedJSON(c *fiber.Ctx, namespace string) (bool, error) {
	if h.redisClient == nil || strings.ToUpper(c.Method()) != fiber.MethodGet {
		return false, nil
	}
	setNoStoreCacheHeaders(c)

	key := buildRedisCacheKey(namespace, c)
	ctx, cancel := context.WithTimeout(context.Background(), redisCacheLookupTimeout)
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
	setNoStoreCacheHeaders(c)
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

	ctx, cancel := context.WithTimeout(context.Background(), redisCacheWriteTimeout)
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

	totalDeleted := 0
	for pass := 1; pass <= redisCacheDeletePasses; pass++ {
		deletedInPass, err := deleteRedisPatternPass(redisClient, pattern)
		if err != nil {
			log.Printf("warning: redis cache invalidation pass failed for pattern %q: %v", pattern, err)
			return
		}
		totalDeleted += deletedInPass
		if deletedInPass == 0 {
			break
		}
	}

	if totalDeleted > 0 {
		log.Printf("redis cache invalidated: pattern=%s deleted=%d", pattern, totalDeleted)
	}
}

func deleteRedisPatternPass(redisClient *redis.Client, pattern string) (int, error) {
	var cursor uint64
	deleted := 0

	for {
		scanCtx, scanCancel := context.WithTimeout(context.Background(), redisCacheScanTimeout)
		keys, nextCursor, err := redisClient.Scan(scanCtx, cursor, pattern, redisCacheScanCount).Result()
		scanCancel()
		if err != nil {
			return deleted, err
		}

		for start := 0; start < len(keys); start += redisCacheDeleteBatch {
			end := start + redisCacheDeleteBatch
			if end > len(keys) {
				end = len(keys)
			}
			batch := keys[start:end]

			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), redisCacheDeleteTimeout)
			deleteErr := redisClient.Unlink(deleteCtx, batch...).Err()
			if deleteErr != nil {
				deleteErr = redisClient.Del(deleteCtx, batch...).Err()
			}
			deleteCancel()
			if deleteErr != nil {
				return deleted, deleteErr
			}
			deleted += len(batch)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return deleted, nil
}

func buildRedisCacheKey(namespace string, c *fiber.Ctx) string {
	queryPairs := make([]string, 0, len(c.Queries()))
	for key, value := range c.Queries() {
		queryPairs = append(queryPairs, fmt.Sprintf("%s=%s", url.QueryEscape(key), url.QueryEscape(value)))
	}
	sort.Strings(queryPairs)

	queryPart := strings.Join(queryPairs, "&")
	if queryPart == "" {
		queryPart = "-"
	}

	return fmt.Sprintf("%s%s:%s:%s", cacheKeyPrefix, namespace, c.Path(), queryPart)
}

func setNoStoreCacheHeaders(c *fiber.Ctx) {
	c.Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
	c.Set("Pragma", "no-cache")
	c.Set("Expires", "0")
}
