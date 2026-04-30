package handlers

import (
	"context"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type BlogHandler struct {
	postRepo     *repository.BlogPostRepository
	categoryRepo *repository.BlogCategoryRepository
}

func NewBlogHandler(postRepo *repository.BlogPostRepository, categoryRepo *repository.BlogCategoryRepository) *BlogHandler {
	return &BlogHandler{postRepo: postRepo, categoryRepo: categoryRepo}
}

func (h *BlogHandler) RegisterRoutes(app *fiber.App) {
	blog := app.Group("/blog")
	blog.Get("/categories", h.ListCategories)
	blog.Get("/posts/popular", h.ListPopularPosts)
	blog.Get("/posts", h.ListPosts)
	blog.Get("/posts/:id", h.GetPost)
}

func (h *BlogHandler) ListCategories(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cats, err := h.categoryRepo.List(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch categories"})
	}
	if cats == nil {
		cats = []models.BlogCategory{}
	}
	return c.JSON(fiber.Map{"data": cats})
}

func (h *BlogHandler) ListPosts(c *fiber.Ctx) error {
	page := c.QueryInt("page", 1)
	limit := c.QueryInt("limit", 12)
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 12
	}

	var categoryID *primitive.ObjectID
	if catStr := c.Query("category"); catStr != "" {
		id, err := primitive.ObjectIDFromHex(catStr)
		if err == nil {
			categoryID = &id
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	posts, total, err := h.postRepo.List(ctx, categoryID, page, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch posts"})
	}
	if posts == nil {
		posts = []models.BlogPost{}
	}
	return c.JSON(fiber.Map{"data": posts, "total": total, "page": page, "limit": limit})
}

func (h *BlogHandler) ListPopularPosts(c *fiber.Ctx) error {
	limit := c.QueryInt("limit", 5)
	if limit < 1 || limit > 20 {
		limit = 5
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	posts, err := h.postRepo.ListPopular(ctx, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch popular posts"})
	}
	if posts == nil {
		posts = []models.BlogPost{}
	}
	return c.JSON(fiber.Map{"data": posts})
}

func (h *BlogHandler) GetPost(c *fiber.Ctx) error {
	id, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid post id"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	post, err := h.postRepo.FindByID(ctx, id)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "post not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}
	go func() {
		_ = h.postRepo.IncrementViews(context.Background(), id)
	}()
	return c.JSON(post)
}
