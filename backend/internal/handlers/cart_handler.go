package handlers

import (
	"context"
	"time"

	"topar/backend/internal/middleware"
	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type CartHandler struct {
	cartRepo        *repository.CartRepository
	mainProductRepo *repository.MainProductRepository
	jwtSecret       string
}

func NewCartHandler(
	cartRepo *repository.CartRepository,
	mainProductRepo *repository.MainProductRepository,
	jwtSecret string,
) *CartHandler {
	return &CartHandler{cartRepo: cartRepo, mainProductRepo: mainProductRepo, jwtSecret: jwtSecret}
}

func (h *CartHandler) RegisterRoutes(app *fiber.App) {
	auth := middleware.Auth(h.jwtSecret)
	cart := app.Group("/cart", auth)
	cart.Get("", h.GetCart)
	cart.Post("/items", h.AddItem)
	cart.Put("/items/:productId", h.UpdateItemQty)
	cart.Delete("/items/:productId", h.RemoveItem)
	cart.Delete("", h.ClearCart)
}

func (h *CartHandler) currentUserID(c *fiber.Ctx) (primitive.ObjectID, error) {
	raw, _ := c.Locals("userID").(string)
	return primitive.ObjectIDFromHex(raw)
}

func (h *CartHandler) GetCart(c *fiber.Ctx) error {
	userID, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cart, err := h.cartRepo.Upsert(ctx, userID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch cart"})
	}
	return c.JSON(cart)
}

type addItemRequest struct {
	ProductID string `json:"productId"`
	Quantity  int    `json:"quantity"`
}

func (h *CartHandler) AddItem(c *fiber.Ctx) error {
	userID, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	var req addItemRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	productID, err := primitive.ObjectIDFromHex(req.ProductID)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid productId"})
	}
	if req.Quantity < 1 {
		req.Quantity = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	product, err := h.mainProductRepo.FindByID(ctx, productID)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "product not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}

	if _, err := h.cartRepo.Upsert(ctx, userID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to initialize cart"})
	}

	item := models.CartItem{
		ProductID:  productID,
		Quantity:   req.Quantity,
		PriceAtAdd: product.Price,
	}
	if err := h.cartRepo.AddItem(ctx, userID, item); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to add item"})
	}

	cart, _ := h.cartRepo.FindByUserID(ctx, userID)
	return c.JSON(cart)
}

type updateQtyRequest struct {
	Quantity int `json:"quantity"`
}

func (h *CartHandler) UpdateItemQty(c *fiber.Ctx) error {
	userID, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	productID, err := primitive.ObjectIDFromHex(c.Params("productId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid productId"})
	}
	var req updateQtyRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if req.Quantity < 1 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "quantity must be at least 1"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.cartRepo.UpdateItemQty(ctx, userID, productID, req.Quantity); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update quantity"})
	}
	cart, _ := h.cartRepo.FindByUserID(ctx, userID)
	return c.JSON(cart)
}

func (h *CartHandler) RemoveItem(c *fiber.Ctx) error {
	userID, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	productID, err := primitive.ObjectIDFromHex(c.Params("productId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid productId"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.cartRepo.RemoveItem(ctx, userID, productID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to remove item"})
	}
	cart, _ := h.cartRepo.FindByUserID(ctx, userID)
	return c.JSON(cart)
}

func (h *CartHandler) ClearCart(c *fiber.Ctx) error {
	userID, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.cartRepo.Clear(ctx, userID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to clear cart"})
	}
	return c.JSON(fiber.Map{"message": "cart cleared"})
}
