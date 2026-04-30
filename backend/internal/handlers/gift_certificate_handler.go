package handlers

import (
	"context"
	"time"

	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type GiftCertificateHandler struct {
	repo *repository.GiftCertificateRepository
}

func NewGiftCertificateHandler(repo *repository.GiftCertificateRepository) *GiftCertificateHandler {
	return &GiftCertificateHandler{repo: repo}
}

func (h *GiftCertificateHandler) RegisterRoutes(app *fiber.App) {
	g := app.Group("/gift-certificates")
	g.Get("", h.ListActive)
	g.Get("/:id", h.GetByID)
}

func (h *GiftCertificateHandler) ListActive(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	certs, err := h.repo.ListActive(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch gift certificates"})
	}
	return c.JSON(fiber.Map{"data": certs})
}

func (h *GiftCertificateHandler) GetByID(c *fiber.Ctx) error {
	id, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid id"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cert, err := h.repo.FindByID(ctx, id)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "gift certificate not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}
	return c.JSON(cert)
}
