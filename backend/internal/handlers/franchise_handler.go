package handlers

import (
	"context"
	"time"

	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type FranchiseHandler struct {
	repo *repository.FranchiseRepository
}

func NewFranchiseHandler(repo *repository.FranchiseRepository) *FranchiseHandler {
	return &FranchiseHandler{repo: repo}
}

func (h *FranchiseHandler) RegisterRoutes(app *fiber.App) {
	f := app.Group("/franchises")
	f.Get("", h.List)
	f.Get("/cities", h.ListCities)
	f.Get("/:id", h.GetByID)
}

func (h *FranchiseHandler) List(c *fiber.Ctx) error {
	city := c.Query("city")
	district := c.Query("district")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	franchises, err := h.repo.List(ctx, city, district)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch franchises"})
	}
	return c.JSON(fiber.Map{"data": franchises})
}

func (h *FranchiseHandler) ListCities(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cities, err := h.repo.DistinctCities(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch cities"})
	}
	return c.JSON(fiber.Map{"data": cities})
}

func (h *FranchiseHandler) GetByID(c *fiber.Ctx) error {
	id, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid id"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	franchise, err := h.repo.FindByID(ctx, id)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "franchise not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}
	return c.JSON(franchise)
}
