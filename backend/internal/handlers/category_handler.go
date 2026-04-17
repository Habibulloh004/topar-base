package handlers

import (
	"context"
	"errors"
	"strings"
	"time"
	"topar/backend/internal/models"
	"topar/backend/internal/repository"
	"topar/backend/internal/services"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type CategoryHandler struct {
	repo            *repository.CategoryRepository
	mainProductRepo *repository.MainProductRepository
	categoryLinker  *services.CategoryLinker
	redisClient     *redis.Client
}

func NewCategoryHandler(
	repo *repository.CategoryRepository,
	mainProductRepo *repository.MainProductRepository,
	categoryLinker *services.CategoryLinker,
	redisClient *redis.Client,
) *CategoryHandler {
	return &CategoryHandler{
		repo:            repo,
		mainProductRepo: mainProductRepo,
		categoryLinker:  categoryLinker,
		redisClient:     redisClient,
	}
}

func (h *CategoryHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/categories", h.GetCategories)
	app.Post("/categories", h.CreateCategory)
	app.Put("/categories/:id", h.UpdateCategory)
	app.Delete("/categories/:id", h.DeleteCategory)
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

type createCategoryRequest struct {
	Name     string `json:"name"`
	ParentID string `json:"parentId"`
}

type updateCategoryRequest struct {
	Name     *string `json:"name"`
	ParentID *string `json:"parentId"`
}

func (h *CategoryHandler) CreateCategory(c *fiber.Ctx) error {
	var req createCategoryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name is required"})
	}

	parentID := strings.TrimSpace(req.ParentID)
	if parentID == "" {
		parentID = "0"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	categories, err := h.repo.ListAll(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if parentID != "0" && !categoryIDExists(categories, parentID) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "parent category not found"})
	}

	id, err := h.repo.Create(ctx, name, parentID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "Category created",
		"id":      id.Hex(),
	})
}

func (h *CategoryHandler) UpdateCategory(c *fiber.Ctx) error {
	categoryID, err := parseCategoryID(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	var req updateCategoryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	categories, err := h.repo.ListAll(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	categoryHex := categoryID.Hex()
	if !categoryIDExists(categories, categoryHex) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "category not found"})
	}

	updates := bson.M{}
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name is required"})
		}
		updates["name"] = name
	}
	if req.ParentID != nil {
		parentID := strings.TrimSpace(*req.ParentID)
		if parentID == "" {
			parentID = "0"
		}
		if parentID == categoryHex {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "category cannot be its own parent"})
		}
		if parentID != "0" && !categoryIDExists(categories, parentID) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "parent category not found"})
		}

		descendants := getCategoryDescendantIDSet(categories, categoryHex)
		if _, isDescendant := descendants[parentID]; isDescendant {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cannot move category into its own subtree"})
		}

		updates["parentId"] = parentID
	}

	if len(updates) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no fields to update"})
	}

	if err := h.repo.UpdateFields(ctx, categoryID, updates); err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "category not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if err := h.refreshMainProductCategoryPaths(ctx, categoryID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{"message": "Category updated"})
}

func (h *CategoryHandler) DeleteCategory(c *fiber.Ctx) error {
	categoryID, err := parseCategoryID(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	categories, err := h.repo.ListAll(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	rootHex := categoryID.Hex()
	if !categoryIDExists(categories, rootHex) {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "category not found"})
	}

	affectedSet := getCategoryDescendantIDSet(categories, rootHex)
	affectedIDs := make([]primitive.ObjectID, 0, len(affectedSet)+1)
	affectedIDs = append(affectedIDs, categoryID)
	for id := range affectedSet {
		oid, parseErr := primitive.ObjectIDFromHex(id)
		if parseErr != nil || oid.IsZero() {
			continue
		}
		affectedIDs = append(affectedIDs, oid)
	}

	deletedCount, err := h.repo.DeleteWithDescendants(ctx, categoryID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if deletedCount == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "category not found"})
	}
	if h.mainProductRepo != nil {
		if _, _, clearErr := h.mainProductRepo.RemoveCategoryByCategoryIDs(ctx, affectedIDs); clearErr != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": clearErr.Error()})
		}
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message":      "Category deleted",
		"deletedCount": deletedCount,
	})
}

func parseCategoryID(value string) (primitive.ObjectID, error) {
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

func categoryIDExists(categories []models.Category, id string) bool {
	for _, category := range categories {
		if category.ID.Hex() == id {
			return true
		}
	}
	return false
}

func getCategoryDescendantIDSet(categories []models.Category, rootID string) map[string]struct{} {
	childrenByParent := make(map[string][]string, len(categories))
	for _, category := range categories {
		childrenByParent[category.ParentID] = append(childrenByParent[category.ParentID], category.ID.Hex())
	}

	result := make(map[string]struct{})
	stack := append([]string(nil), childrenByParent[rootID]...)
	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if _, exists := result[current]; exists {
			continue
		}
		result[current] = struct{}{}
		stack = append(stack, childrenByParent[current]...)
	}

	return result
}

func (h *CategoryHandler) refreshMainProductCategoryPaths(ctx context.Context, categoryID primitive.ObjectID) error {
	if h.mainProductRepo == nil || h.categoryLinker == nil || categoryID.IsZero() {
		return nil
	}

	if err := h.categoryLinker.BuildCache(ctx); err != nil {
		return err
	}

	affected := h.categoryLinker.GetCategoryAndDescendantIDs(categoryID)
	if len(affected) == 0 {
		affected = []primitive.ObjectID{categoryID}
	}

	pathsByCategoryID := make(map[primitive.ObjectID][]string, len(affected))
	for _, currentID := range affected {
		if currentID.IsZero() {
			continue
		}
		path := h.categoryLinker.GetCategoryPath(currentID)
		if len(path) == 0 {
			return errors.New("failed to resolve updated category path")
		}
		pathsByCategoryID[currentID] = append([]string{}, path...)
	}

	_, _, err := h.mainProductRepo.RefreshCategoryPathsByCategoryID(ctx, pathsByCategoryID)
	return err
}

func (h *CategoryHandler) invalidateProductCaches() {
	invalidateProductCachesByRedis(h.redisClient)
}
