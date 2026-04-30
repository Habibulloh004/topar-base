package config

import (
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	MongoURI       string
	MongoDatabase  string
	ServerPort     string
	CategoriesFile string
	UploadsDir     string
	RedisEnabled   bool
	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	RedisCacheTTL  time.Duration
	EksmoAPIKey    string
	EksmoAPIKeys   []string
	EksmoBaseURL   string
	EksmoPerPage   int
	EksmoMaxPages  int
	EksmoTimeoutS  int
	EksmoRetries   int
	EksmoBackoffMS int

	BillzSyncEnabled bool
	BillzAuthURL     string
	BillzURL         string
	BillzAPISecret   string
	BillzTargetShop  string
	BillzTimeoutS    int
	BillzSyncEvery   time.Duration

	JWTSecret string
}

func Load() Config {
	eksmoAPIKey := getEnv("EKSMO_API_KEY", "1f7fab12215b7d85c62d2492ed66750d")
	eksmoAPIKeys := getEnvCSV("EKSMO_API_KEYS")
	if len(eksmoAPIKeys) == 0 && strings.TrimSpace(eksmoAPIKey) != "" {
		eksmoAPIKeys = []string{strings.TrimSpace(eksmoAPIKey)}
	}

	jwtSecret := normalizeEnvValue(os.Getenv("JWT_SECRET"))
	if jwtSecret == "" {
		log.Fatal("JWT_SECRET must be set")
	}

	return Config{
		MongoURI:       getEnv("MONGODB_URI", "mongodb://localhost:27017"),
		MongoDatabase:  getEnv("MONGODB_DATABASE", "topar_db"),
		ServerPort:     getEnv("PORT", "8090"),
		CategoriesFile: getEnv("CATEGORIES_FILE", ""),
		UploadsDir:     getEnv("UPLOADS_DIR", "./uploads"),
		RedisEnabled:   getEnvBool("REDIS_ENABLED", false),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  getEnv("REDIS_PASSWORD", ""),
		RedisDB:        getEnvInt("REDIS_DB", 0),
		RedisCacheTTL:  getEnvCacheTTL("REDIS_CACHE_TTL", 0),
		EksmoAPIKey:    eksmoAPIKey,
		EksmoAPIKeys:   eksmoAPIKeys,
		EksmoBaseURL:   getEnv("EKSMO_BASE_URL", "https://api3.eksmo.ru"),
		EksmoPerPage:   getEnvInt("EKSMO_PER_PAGE", 500),
		EksmoMaxPages:  getEnvInt("EKSMO_MAX_PAGES", 0),
		EksmoTimeoutS:  getEnvInt("EKSMO_TIMEOUT_SECONDS", 300),
		EksmoRetries:   getEnvInt("EKSMO_RETRIES", 6),
		EksmoBackoffMS: getEnvInt("EKSMO_BACKOFF_MS", 1500),

		BillzSyncEnabled: getEnvBool("BILLZ_SYNC_ENABLED", true),
		BillzAuthURL:     getEnv("BILLZ_AUTH_URL", "https://api-admin.billz.ai/v1/auth"),
		BillzURL:         getEnv("BILLZ_URL", "https://api-admin.billz.ai/v2"),
		BillzAPISecret:   getEnv("BILLZ_API_SECRET_KEY", "9f6ad7159637e4f65dc9a8ad6b619545c41eadfeaaba32fea554b30f8c842b0983982fc680f59cd58536f954d3c137998deea90385d8987088a58cb52f13dbac5c34f9d5ca1f4d2085b774a6ab7f1f5cc9e4f7b65cd061486c66507a4644219d5825fb8f622fba0053233666611afb9c6a298ea196e4c03c"),
		BillzTargetShop:  getEnv("BILLZ_TARGET_SHOP", "topar"),
		BillzTimeoutS:    getEnvInt("BILLZ_TIMEOUT_SECONDS", 60),
		BillzSyncEvery:   getEnvDuration("BILLZ_SYNC_INTERVAL", time.Hour),

		JWTSecret: jwtSecret,
	}
}

func getEnv(key, fallback string) string {
	if value := normalizeEnvValue(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	value := normalizeEnvValue(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvBool(key string, fallback bool) bool {
	value := normalizeEnvValue(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	value := normalizeEnvValue(os.Getenv(key))
	if value == "" {
		return fallback
	}

	parsed, err := time.ParseDuration(value)
	if err == nil && parsed > 0 {
		return parsed
	}

	seconds, err := strconv.Atoi(value)
	if err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}

	return fallback
}

func getEnvCacheTTL(key string, fallback time.Duration) time.Duration {
	value := strings.ToLower(normalizeEnvValue(os.Getenv(key)))
	if value == "" {
		return fallback
	}

	// 0/never/infinite means persist until explicit invalidation.
	if value == "0" || value == "never" || value == "infinite" {
		return 0
	}

	parsed, err := time.ParseDuration(value)
	if err == nil && parsed >= 0 {
		return parsed
	}

	seconds, err := strconv.Atoi(value)
	if err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}

	return fallback
}

func getEnvCSV(key string) []string {
	value := normalizeEnvValue(os.Getenv(key))
	if strings.TrimSpace(value) == "" {
		return nil
	}

	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func normalizeEnvValue(value string) string {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) >= 2 {
		if (trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"') ||
			(trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'') {
			return strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		}
	}
	return trimmed
}
