package handlers

import (
	"context"
	"strings"
	"time"

	"topar/backend/internal/middleware"
	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/crypto/bcrypt"
)

type UserHandler struct {
	userRepo    *repository.UserRepository
	addressRepo *repository.UserAddressRepository
	orderRepo   *repository.OrderRepository
	bookRepo    *repository.UserBookRepository
	jwtSecret   string
}

func NewUserHandler(
	userRepo *repository.UserRepository,
	addressRepo *repository.UserAddressRepository,
	orderRepo *repository.OrderRepository,
	bookRepo *repository.UserBookRepository,
	jwtSecret string,
) *UserHandler {
	return &UserHandler{
		userRepo:    userRepo,
		addressRepo: addressRepo,
		orderRepo:   orderRepo,
		bookRepo:    bookRepo,
		jwtSecret:   jwtSecret,
	}
}

func (h *UserHandler) RegisterRoutes(app *fiber.App) {
	auth := middleware.Auth(h.jwtSecret)
	me := app.Group("/users/me", auth)
	me.Get("", h.GetMe)
	me.Put("", h.UpdateMe)
	me.Put("/password", h.ChangePassword)
	me.Get("/addresses", h.GetAddresses)
	me.Post("/addresses", h.CreateAddress)
	me.Put("/addresses/:id", h.UpdateAddress)
	me.Delete("/addresses/:id", h.DeleteAddress)
	me.Get("/orders", h.GetMyOrders)
	me.Get("/books", h.GetMyBooks)
	me.Delete("/books/:productId", h.RemoveBook)
}

func (h *UserHandler) currentUserID(c *fiber.Ctx) (primitive.ObjectID, error) {
	raw, _ := c.Locals("userID").(string)
	return primitive.ObjectIDFromHex(raw)
}

func (h *UserHandler) GetMe(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user, err := h.userRepo.FindByID(ctx, id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "user not found"})
	}
	return c.JSON(user)
}

type updateMeRequest struct {
	FirstName   *string `json:"firstName"`
	LastName    *string `json:"lastName"`
	DisplayName *string `json:"displayName"`
	Phone       *string `json:"phone"`
}

func (h *UserHandler) UpdateMe(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	var req updateMeRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	fields := bson.M{}
	if req.FirstName != nil {
		fields["firstName"] = strings.TrimSpace(*req.FirstName)
	}
	if req.LastName != nil {
		fields["lastName"] = strings.TrimSpace(*req.LastName)
	}
	if req.DisplayName != nil {
		fields["displayName"] = strings.TrimSpace(*req.DisplayName)
	}
	if req.Phone != nil {
		fields["phone"] = strings.TrimSpace(*req.Phone)
	}
	if len(fields) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no fields to update"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.userRepo.UpdateFields(ctx, id, fields); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update user"})
	}
	user, _ := h.userRepo.FindByID(ctx, id)
	return c.JSON(user)
}

type changePasswordRequest struct {
	OldPassword string `json:"oldPassword"`
	NewPassword string `json:"newPassword"`
}

func (h *UserHandler) ChangePassword(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	var req changePasswordRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if req.OldPassword == "" || req.NewPassword == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "oldPassword and newPassword are required"})
	}
	if len(req.NewPassword) < 6 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "new password must be at least 6 characters"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	user, err := h.userRepo.FindByID(ctx, id)
	if err != nil {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "user not found"})
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.OldPassword)); err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "incorrect current password"})
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to hash password"})
	}
	if err := h.userRepo.UpdateFields(ctx, id, bson.M{"passwordHash": string(hash)}); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update password"})
	}
	return c.JSON(fiber.Map{"message": "password updated"})
}

func (h *UserHandler) GetAddresses(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addresses, err := h.addressRepo.FindByUserID(ctx, id)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch addresses"})
	}
	if addresses == nil {
		addresses = []models.UserAddress{}
	}
	return c.JSON(fiber.Map{"data": addresses})
}

type addressRequest struct {
	Type        string `json:"type"`
	City        string `json:"city"`
	District    string `json:"district"`
	AddressText string `json:"addressText"`
	IsDefault   bool   `json:"isDefault"`
}

func (h *UserHandler) CreateAddress(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	var req addressRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if strings.TrimSpace(req.AddressText) == "" || strings.TrimSpace(req.City) == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "city and addressText are required"})
	}
	addrType := req.Type
	if addrType == "" {
		addrType = "other"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addr := &models.UserAddress{
		UserID:      id,
		Type:        addrType,
		City:        strings.TrimSpace(req.City),
		District:    strings.TrimSpace(req.District),
		AddressText: strings.TrimSpace(req.AddressText),
		IsDefault:   req.IsDefault,
	}
	if err := h.addressRepo.Create(ctx, addr); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create address"})
	}
	return c.Status(fiber.StatusCreated).JSON(addr)
}

func (h *UserHandler) UpdateAddress(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	addrID, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid address id"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	existing, err := h.addressRepo.FindByID(ctx, addrID)
	if err != nil || existing.UserID != id {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "address not found"})
	}
	var req addressRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	fields := bson.M{"isDefault": req.IsDefault}
	if req.Type != "" {
		fields["type"] = req.Type
	}
	if req.City != "" {
		fields["city"] = strings.TrimSpace(req.City)
	}
	if req.District != "" {
		fields["district"] = strings.TrimSpace(req.District)
	}
	if req.AddressText != "" {
		fields["addressText"] = strings.TrimSpace(req.AddressText)
	}
	if err := h.addressRepo.Update(ctx, addrID, fields); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to update address"})
	}
	updated, _ := h.addressRepo.FindByID(ctx, addrID)
	return c.JSON(updated)
}

func (h *UserHandler) DeleteAddress(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	addrID, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid address id"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	existing, err := h.addressRepo.FindByID(ctx, addrID)
	if err != nil || existing.UserID != id {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "address not found"})
	}
	if err := h.addressRepo.Delete(ctx, addrID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to delete address"})
	}
	return c.JSON(fiber.Map{"message": "address deleted"})
}

func (h *UserHandler) GetMyOrders(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	page := c.QueryInt("page", 1)
	limit := c.QueryInt("limit", 20)
	if page < 1 {
		page = 1
	}
	if limit < 1 || limit > 100 {
		limit = 20
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	orders, total, err := h.orderRepo.FindByUserID(ctx, id, page, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch orders"})
	}
	if orders == nil {
		orders = []models.Order{}
	}
	return c.JSON(fiber.Map{"data": orders, "total": total, "page": page, "limit": limit})
}

func (h *UserHandler) GetMyBooks(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	books, err := h.bookRepo.FindByUserID(ctx, id)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch books"})
	}
	if books == nil {
		books = []models.UserBook{}
	}
	return c.JSON(fiber.Map{"data": books})
}

func (h *UserHandler) RemoveBook(c *fiber.Ctx) error {
	id, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	productID, err := primitive.ObjectIDFromHex(c.Params("productId"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid productId"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.bookRepo.Delete(ctx, id, productID); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to remove book"})
	}
	return c.JSON(fiber.Map{"message": "book removed from library"})
}
