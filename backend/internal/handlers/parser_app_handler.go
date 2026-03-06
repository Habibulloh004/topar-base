package handlers

import (
	"context"
	"strings"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"
	"topar/backend/internal/services"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ParserAppHandler struct {
	repo    *repository.ParserAppRepository
	service *services.ParserAppService
	redis   *redis.Client
}

func NewParserAppHandler(
	repo *repository.ParserAppRepository,
	service *services.ParserAppService,
	redisClient *redis.Client,
) *ParserAppHandler {
	return &ParserAppHandler{
		repo:    repo,
		service: service,
		redis:   redisClient,
	}
}

func (h *ParserAppHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/parser-app/schema", h.GetSchema)
	app.Get("/parser-app/runs", h.ListRuns)
	app.Get("/parser-app/runs/:id", h.GetRun)
	app.Get("/parser-app/runs/:id/records", h.GetRunRecords)
	app.Post("/parser-app/parse", h.Parse)
	app.Get("/parser-app/mappings", h.ListMappings)
	app.Post("/parser-app/mappings", h.SaveMapping)
	app.Post("/parser-app/sync-local", h.SyncLocalRecords)
	app.Post("/parser-app/runs/:id/sync", h.SyncRun)
	app.Post("/parser-app/runs/:id/seed", h.SeedRun)
}

func (h *ParserAppHandler) GetSchema(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"target": h.service.GetTargetSchema(),
	})
}

func (h *ParserAppHandler) ListRuns(c *fiber.Ctx) error {
	limit := int64(parseIntQuery(c, "limit", 20))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	runs, err := h.repo.ListRuns(ctx, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"items": runs})
}

func (h *ParserAppHandler) GetRun(c *fiber.Ctx) error {
	runID, err := parseObjectIDParam(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	run, exists, err := h.repo.GetRun(ctx, runID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "run not found"})
	}

	limit := int64(parseIntQuery(c, "limit", 20))
	records, total, err := h.repo.ListRunRecords(ctx, runID, 1, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	if len(run.DetectedFields) == 0 {
		fields, detectErr := h.repo.DetectRunFields(ctx, runID, 500)
		if detectErr == nil {
			run.DetectedFields = fields
		}
	}

	return c.JSON(fiber.Map{
		"run":     run,
		"total":   total,
		"records": records,
	})
}

func (h *ParserAppHandler) GetRunRecords(c *fiber.Ctx) error {
	runID, err := parseObjectIDParam(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	records, total, err := h.repo.ListRunRecords(ctx, runID, page, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"items": records,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

func (h *ParserAppHandler) Parse(c *fiber.Ctx) error {
	var req services.ParserParseRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	result, err := h.service.ParseAndStore(ctx, req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"run":    result.Run,
		"sample": result.Sample,
	})
}

func (h *ParserAppHandler) ListMappings(c *fiber.Ctx) error {
	limit := int64(parseIntQuery(c, "limit", 20))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	profiles, err := h.repo.ListMappingProfiles(ctx, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"items": profiles})
}

func (h *ParserAppHandler) SaveMapping(c *fiber.Ctx) error {
	var payload struct {
		Name  string                            `json:"name"`
		Rules map[string]models.ParserFieldRule `json:"rules"`
	}
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if len(payload.Rules) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "rules are required"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	profile, err := h.repo.SaveMappingProfile(ctx, strings.TrimSpace(payload.Name), payload.Rules)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(profile)
}

func (h *ParserAppHandler) SyncRun(c *fiber.Ctx) error {
	return h.sync(c, false)
}

func (h *ParserAppHandler) SeedRun(c *fiber.Ctx) error {
	return h.sync(c, true)
}

func (h *ParserAppHandler) SyncLocalRecords(c *fiber.Ctx) error {
	var req services.ParserLocalSyncRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	result, err := h.service.SyncLocalRecords(ctx, req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	invalidateProductCachesByRedis(h.redis)
	return c.JSON(result)
}

func (h *ParserAppHandler) sync(c *fiber.Ctx, seedingMode bool) error {
	runID, err := parseObjectIDParam(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	var req services.ParserSyncRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if seedingMode {
		req.SyncEksmo = true
		req.SyncMain = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	result, err := h.service.SyncRun(ctx, runID, req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	invalidateProductCachesByRedis(h.redis)
	return c.JSON(result)
}

func parseObjectIDParam(value string) (primitive.ObjectID, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return primitive.NilObjectID, fiber.NewError(fiber.StatusBadRequest, "id is required")
	}
	id, err := primitive.ObjectIDFromHex(trimmed)
	if err != nil {
		return primitive.NilObjectID, fiber.NewError(fiber.StatusBadRequest, "id is invalid")
	}
	return id, nil
}
