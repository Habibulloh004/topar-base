package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"topar/backend/internal/config"
	"topar/backend/internal/db"
	"topar/backend/internal/handlers"
	"topar/backend/internal/repository"
	"topar/backend/internal/services"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {
	loadEnvFiles()
	cfg := config.Load()
	uploadsDir := strings.TrimSpace(cfg.UploadsDir)
	if uploadsDir == "" {
		uploadsDir = "./uploads"
	}

	if err := os.MkdirAll(filepath.Join(uploadsDir, "main-products"), 0o755); err != nil {
		log.Printf(
			"warning: failed to prepare uploads directory %q at startup: %v (image uploads may fail until permissions are fixed)",
			uploadsDir,
			err,
		)
	}

	client, err := db.Connect(context.Background(), cfg.MongoURI)
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	var redisClient *redis.Client
	var redisReady bool
	if cfg.RedisEnabled {
		connectedRedis, redisErr := db.ConnectRedis(context.Background(), cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
		if redisErr != nil {
			log.Printf("warning: failed to connect to Redis at %s (cache disabled): %v", cfg.RedisAddr, redisErr)
		} else {
			redisClient = connectedRedis
			redisReady = true
		}
	}

	database := client.Database(cfg.MongoDatabase)

	// Existing repositories
	categoryRepo := repository.NewCategoryRepository(database)
	eksmoRepo := repository.NewEksmoProductRepository(database)
	mainProductRepo := repository.NewMainProductRepository(database)
	syncStateRepo := repository.NewSyncStateRepository(database)
	parserRepo := repository.NewParserAppRepository(database)

	// New entity repositories
	authorRepo := repository.NewEksmoAuthorRepository(database)
	tagRepo := repository.NewEksmoTagRepository(database)
	seriesRepo := repository.NewEksmoSeriesRepository(database)
	publisherRepo := repository.NewEksmoPublisherRepository(database)
	subjectRepo := repository.NewEksmoSubjectRepository(database)
	nicheRepo := repository.NewEksmoNicheRepository(database)

	// Services
	eksmoService := services.NewEksmoService(cfg, syncStateRepo)
	categoryLinker := services.NewCategoryLinker(categoryRepo)
	treeBuilder := services.NewEksmoTreeBuilder(nicheRepo, subjectRepo)
	billzSyncService := services.NewBillzSyncService(cfg, mainProductRepo)
	parserService := services.NewParserAppService(parserRepo, eksmoRepo, mainProductRepo)

	// Handlers
	categoryHandler := handlers.NewCategoryHandler(categoryRepo)
	eksmoHandler := handlers.NewEksmoProductHandler(
		eksmoRepo,
		eksmoService,
		categoryLinker,
		authorRepo,
		tagRepo,
		seriesRepo,
		publisherRepo,
		subjectRepo,
		nicheRepo,
		treeBuilder,
		mainProductRepo,
		billzSyncService,
		uploadsDir,
		redisClient,
		cfg.RedisCacheTTL,
	)
	parserHandler := handlers.NewParserAppHandler(parserRepo, parserService, redisClient)

	app := fiber.New(fiber.Config{
		BodyLimit: 64 * 1024 * 1024, // Allow multiple high-resolution image uploads in one request.
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			requestID := strings.TrimSpace(fmt.Sprint(c.Locals("requestid")))
			if requestID == "" || requestID == "<nil>" {
				requestID = c.Get(fiber.HeaderXRequestID)
			}

			var fiberErr *fiber.Error
			if errors.As(err, &fiberErr) {
				log.Printf(
					"[http_error] request_id=%s method=%s path=%s status=%d ip=%s error=%q",
					requestID,
					c.Method(),
					c.OriginalURL(),
					fiberErr.Code,
					c.IP(),
					fiberErr.Message,
				)
				return c.Status(fiberErr.Code).JSON(fiber.Map{"error": fiberErr.Message})
			}
			log.Printf(
				"[http_error] request_id=%s method=%s path=%s status=%d ip=%s error=%q",
				requestID,
				c.Method(),
				c.OriginalURL(),
				fiber.StatusInternalServerError,
				c.IP(),
				err.Error(),
			)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "internal server error"})
		},
	})
	app.Use(cors.New())
	app.Use(requestid.New())
	app.Use(logger.New(logger.Config{
		Format:     "[http] ${time} request_id=${locals:requestid} status=${status} latency=${latency} ip=${ip} ${method} ${path} bytes_in=${bytesReceived} bytes_out=${bytesSent}\n",
		TimeFormat: "2006-01-02T15:04:05.000Z07:00",
		TimeZone:   "UTC",
	}))
	app.Use("/topar-parser-app*", func(c *fiber.Ctx) error {
		c.Set("Cache-Control", "no-store, no-cache, must-revalidate, proxy-revalidate")
		c.Set("Pragma", "no-cache")
		c.Set("Expires", "0")
		return c.Next()
	})
	app.Static("/uploads", uploadsDir)
	if parserAppDir, ok := resolveParserAppDir(); ok {
		app.Static("/topar-parser-app", parserAppDir)
		app.Get("/topar-parser-app", func(c *fiber.Ctx) error {
			return c.SendFile(filepath.Join(parserAppDir, "index.html"))
		})
		log.Printf("serving parser app from: %s", parserAppDir)
	} else {
		log.Printf("warning: topar-parser-app directory was not found; /topar-parser-app will not be served")
	}

	syncCtx, syncCancel := context.WithCancel(context.Background())
	defer syncCancel()
	billzSyncService.Start(syncCtx)

	go func() {
		indexCtx, indexCancel := context.WithTimeout(syncCtx, 45*time.Second)
		defer indexCancel()

		if err := mainProductRepo.EnsureIndexes(indexCtx); err != nil {
			log.Printf("warning: failed to ensure main product indexes: %v", err)
		}
		if err := parserRepo.EnsureIndexes(indexCtx); err != nil {
			log.Printf("warning: failed to ensure parser indexes: %v", err)
		}
	}()

	app.Get("/health", func(c *fiber.Ctx) error {
		return c.JSON(fiber.Map{
			"status":   "ok",
			"database": cfg.MongoDatabase,
			"redis":    redisReady,
		})
	})

	categoryHandler.RegisterRoutes(app)
	eksmoHandler.RegisterRoutes(app)
	parserHandler.RegisterRoutes(app)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
		if redisClient != nil {
			_ = redisClient.Close()
		}
	}()

	addr := fmt.Sprintf(":%s", cfg.ServerPort)
	uploadsDirAbs, err := filepath.Abs(uploadsDir)
	if err != nil {
		uploadsDirAbs = uploadsDir
	}
	log.Printf("backend listening on %s", addr)
	log.Printf("using MongoDB database: %s", cfg.MongoDatabase)
	if cfg.RedisEnabled {
		log.Printf("redis cache enabled: addr=%s db=%d ttl=%s ready=%t", cfg.RedisAddr, cfg.RedisDB, cfg.RedisCacheTTL, redisReady)
	} else {
		log.Printf("redis cache disabled")
	}
	log.Printf("serving uploads from: %s", uploadsDirAbs)
	if err := app.Listen(addr); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func resolveParserAppDir() (string, bool) {
	candidates := []string{
		"topar-parser-app",
		"../topar-parser-app",
	}
	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil || !info.IsDir() {
			continue
		}
		abs, absErr := filepath.Abs(candidate)
		if absErr != nil {
			return candidate, true
		}
		return abs, true
	}
	return "", false
}

func loadEnvFiles() {
	for _, file := range []string{"backend/.env", ".env"} {
		if _, err := os.Stat(file); err != nil {
			continue
		}
		if err := godotenv.Load(file); err != nil {
			log.Printf("warning: failed to load %q: %v", file, err)
		}
	}
}

func startEksmoBootstrapInBackground(
	parentCtx context.Context,
	timeout time.Duration,
	eksmoAPIKey string,
	service *services.EksmoService,
	productRepo *repository.EksmoProductRepository,
	subjectRepo *repository.EksmoSubjectRepository,
	nicheRepo *repository.EksmoNicheRepository,
) {
	go func() {
		if timeout <= 0 {
			timeout = 15 * time.Minute
		}

		ctx, cancel := context.WithTimeout(parentCtx, timeout)
		defer cancel()
		log.Printf("eksmo startup bootstrap started (timeout=%s)", timeout)

		if service == nil || productRepo == nil || subjectRepo == nil || nicheRepo == nil {
			log.Printf("eksmo startup bootstrap skipped: dependencies are not configured")
			return
		}
		if strings.TrimSpace(eksmoAPIKey) == "" {
			log.Printf("eksmo startup bootstrap skipped: EKSMO_API_KEY is empty")
			return
		}

		productsCount, err := productRepo.Count(ctx)
		if err != nil {
			log.Printf("eksmo startup bootstrap count failed (products): %v", err)
			return
		}
		subjectsCount, err := subjectRepo.Count(ctx)
		if err != nil {
			log.Printf("eksmo startup bootstrap count failed (subjects): %v", err)
			return
		}
		nichesCount, err := nicheRepo.Count(ctx)
		if err != nil {
			log.Printf("eksmo startup bootstrap count failed (niches): %v", err)
			return
		}
		log.Printf(
			"eksmo startup bootstrap counts: products=%d subjects=%d niches=%d",
			productsCount,
			subjectsCount,
			nichesCount,
		)

		if productsCount > 0 && subjectsCount > 0 && nichesCount > 0 {
			log.Printf(
				"eksmo startup bootstrap skipped: products=%d subjects=%d niches=%d",
				productsCount,
				subjectsCount,
				nichesCount,
			)
			return
		}

		result, err := service.SyncAllProductsWithExtraction(ctx, services.ProductSyncRepos{
			Products: productRepo,
			Subjects: subjectRepo,
			Niches:   nicheRepo,
		}, services.EksmoSyncOptions{
			PerPage:  500,
			MaxPages: 0,
			Resume:   true,
			Reset:    true,
		})
		if err != nil {
			log.Printf("eksmo startup bootstrap failed in background: %v", err)
			return
		}

		log.Printf(
			"eksmo startup bootstrap completed: fetched=%d upserted=%d modified=%d pages=%d",
			result.Fetched,
			result.Upserted,
			result.Modified,
			result.Pages,
		)
	}()
}
