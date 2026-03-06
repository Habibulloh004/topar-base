package handlers

import (
	"context"
	"time"
	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
)

type CategoryHandler struct {
	repo *repository.CategoryRepository
}

func NewCategoryHandler(repo *repository.CategoryRepository) *CategoryHandler {
	return &CategoryHandler{repo: repo}
}

func (h *CategoryHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/categories", h.GetCategories)
}

func (h *CategoryHandler) GetCategories(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tree, err := h.repo.GetTree(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"collection": "main_categories",
		"data":       tree,
	})
}
