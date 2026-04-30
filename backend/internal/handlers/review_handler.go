package handlers

import (
	"context"
	"errors"
	"time"

	"topar/backend/internal/middleware"
	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type ReviewHandler struct {
	reviewRepo *repository.ReviewRepository
	jwtSecret  string
}

func NewReviewHandler(reviewRepo *repository.ReviewRepository, jwtSecret string) *ReviewHandler {
	return &ReviewHandler{reviewRepo: reviewRepo, jwtSecret: jwtSecret}
}

func (h *ReviewHandler) RegisterRoutes(app *fiber.App) {
	auth := middleware.Auth(h.jwtSecret)
	app.Get("/products/:id/reviews", h.GetProductReviews)
	app.Post("/products/:id/reviews", auth, h.CreateReview)
	app.Put("/reviews/:id", auth, h.UpdateReview)
	app.Delete("/reviews/:id", auth, h.DeleteReview)
}

func (h *ReviewHandler) GetProductReviews(c *fiber.Ctx) error {
	productID, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid product id"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reviews, err := h.reviewRepo.FindByProductID(ctx, productID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch reviews"})
	}
	if reviews == nil {
		reviews = []models.Review{}
	}
	avg, count, _ := h.reviewRepo.AverageRating(ctx, productID)
	return c.JSON(fiber.Map{"data": reviews, "averageRating": avg, "reviewCount": count})
}

type reviewRequest struct {
	Rating  int    `json:"rating"`
	Comment string `json:"comment"`
}

func (h *ReviewHandler) CreateReview(c *fiber.Ctx) error {
	productID, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid product id"})
	}
	raw, _ := c.Locals("userID").(string)
	userID, err := primitive.ObjectIDFromHex(raw)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	var req reviewRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if req.Rating < 1 || req.Rating > 5 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "rating must be between 1 and 5"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if existing, err := h.reviewRepo.FindByUserAndProduct(ctx, userID, productID); err == nil && existing != nil {
		return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "you have already reviewed this product"})
	} else if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}
	review := &models.Review{
		UserID:    userID,
		ProductID: productID,
		Rating:    req.Rating,
		Comment:   req.Comment,
	}
	if err := h.reviewRepo.Create(ctx, review); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "you have already reviewed this product"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create review"})
	}
	return c.Status(fiber.StatusCreated).JSON(review)
}

func (h *ReviewHandler) UpdateReview(c *fiber.Ctx) error {
	reviewID, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid review id"})
	}
	raw, _ := c.Locals("userID").(string)
	userID, err := primitive.ObjectIDFromHex(raw)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	existing, err := h.reviewRepo.FindByID(ctx, reviewID)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "review not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}
	if existing.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "not your review"})
	}
	var req reviewRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if req.Rating < 1 || req.Rating > 5 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "rating must be between 1 and 5"})
	}
	if err := h.reviewRepo.Update(ctx, reviewID, map[string]any{"rating": req.Rating, "comment": req.Comment}); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update review"})
	}
	updated, _ := h.reviewRepo.FindByID(ctx, reviewID)
	return c.JSON(updated)
}

func (h *ReviewHandler) DeleteReview(c *fiber.Ctx) error {
	reviewID, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid review id"})
	}
	raw, _ := c.Locals("userID").(string)
	userID, err := primitive.ObjectIDFromHex(raw)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	existing, err := h.reviewRepo.FindByID(ctx, reviewID)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "review not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}
	if existing.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "not your review"})
	}
	if err := h.reviewRepo.Delete(ctx, reviewID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to delete review"})
	}
	return c.JSON(fiber.Map{"message": "review deleted"})
}
