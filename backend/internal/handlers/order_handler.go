package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	"topar/backend/internal/middleware"
	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type OrderHandler struct {
	orderRepo       *repository.OrderRepository
	cartRepo        *repository.CartRepository
	mainProductRepo *repository.MainProductRepository
	addressRepo     *repository.UserAddressRepository
	userBookRepo    *repository.UserBookRepository
	jwtSecret       string
}

func NewOrderHandler(
	orderRepo *repository.OrderRepository,
	cartRepo *repository.CartRepository,
	mainProductRepo *repository.MainProductRepository,
	addressRepo *repository.UserAddressRepository,
	userBookRepo *repository.UserBookRepository,
	jwtSecret string,
) *OrderHandler {
	return &OrderHandler{
		orderRepo:       orderRepo,
		cartRepo:        cartRepo,
		mainProductRepo: mainProductRepo,
		addressRepo:     addressRepo,
		userBookRepo:    userBookRepo,
		jwtSecret:       jwtSecret,
	}
}

func (h *OrderHandler) RegisterRoutes(app *fiber.App) {
	auth := middleware.Auth(h.jwtSecret)
	orders := app.Group("/orders", auth)
	orders.Post("", h.CreateOrder)
	orders.Get("", h.GetMyOrders)
	orders.Get("/:id", h.GetOrderByID)
}

func (h *OrderHandler) currentUserID(c *fiber.Ctx) (primitive.ObjectID, error) {
	raw, _ := c.Locals("userID").(string)
	return primitive.ObjectIDFromHex(raw)
}

type createOrderRequest struct {
	PaymentMethod     string  `json:"paymentMethod"`
	DeliveryAddressID string  `json:"deliveryAddressId"`
	Comments          string  `json:"comments"`
	DeliveryAmount    float64 `json:"deliveryAmount"`
	BonusAmount       float64 `json:"bonusAmount"`
}

func (h *OrderHandler) CreateOrder(c *fiber.Ctx) error {
	if err := h.validateDependencies(); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	userID, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	var req createOrderRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	req.PaymentMethod = strings.TrimSpace(strings.ToLower(req.PaymentMethod))
	req.Comments = strings.TrimSpace(req.Comments)
	if req.PaymentMethod == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "paymentMethod is required"})
	}
	if req.DeliveryAmount < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "deliveryAmount cannot be negative"})
	}
	if req.BonusAmount < 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "bonusAmount cannot be negative"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cart, err := h.cartRepo.FindByUserID(ctx, userID)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cart is empty"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch cart"})
	}
	if len(cart.Items) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "cart is empty"})
	}

	productIDs := make([]primitive.ObjectID, len(cart.Items))
	for i, item := range cart.Items {
		productIDs[i] = item.ProductID
	}
	products, err := h.mainProductRepo.ListByIDs(ctx, productIDs)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch products"})
	}

	productMap := make(map[primitive.ObjectID]models.MainProduct, len(products))
	for _, p := range products {
		productMap[p.ID] = p
	}

	var orderItems []models.OrderItem
	var totalAmount float64
	for _, item := range cart.Items {
		p, ok := productMap[item.ProductID]
		if !ok {
			continue
		}
		price := item.PriceAtAdd
		if price == 0 {
			price = p.Price
		}
		orderItems = append(orderItems, models.OrderItem{
			ProductID: item.ProductID,
			Name:      p.Name,
			Price:     price,
			Quantity:  item.Quantity,
		})
		totalAmount += price * float64(item.Quantity)
	}
	if len(orderItems) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no valid products in cart"})
	}

	var deliveryAddressID *primitive.ObjectID
	if req.DeliveryAddressID != "" {
		addrID, err := primitive.ObjectIDFromHex(strings.TrimSpace(req.DeliveryAddressID))
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid deliveryAddressId"})
		}
		addr, err := h.addressRepo.FindByID(ctx, addrID)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "delivery address not found"})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to validate delivery address"})
		}
		if addr.UserID != userID {
			return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "delivery address does not belong to the current user"})
		}
		deliveryAddressID = &addrID
	}

	order := &models.Order{
		UserID:            userID,
		Items:             orderItems,
		TotalAmount:       totalAmount,
		DeliveryAmount:    req.DeliveryAmount,
		BonusAmount:       req.BonusAmount,
		PaymentMethod:     req.PaymentMethod,
		Comments:          req.Comments,
		Status:            "pending",
		DeliveryAddressID: deliveryAddressID,
	}

	if err := h.orderRepo.Create(ctx, order); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to create order"})
	}

	originalItems := append([]models.CartItem(nil), cart.Items...)
	if err := h.cartRepo.Clear(ctx, userID); err != nil {
		_ = h.orderRepo.Delete(ctx, order.ID)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to finalize order cart state"})
	}

	createdBookProductIDs := make([]primitive.ObjectID, 0, len(orderItems))
	for _, item := range orderItems {
		exists, err := h.userBookRepo.Exists(ctx, userID, item.ProductID)
		if err != nil {
			h.rollbackOrderCreate(ctx, order.ID, userID, originalItems, createdBookProductIDs)
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to finalize purchased books"})
		}
		if !exists {
			if err := h.userBookRepo.Create(ctx, &models.UserBook{
				UserID:    userID,
				ProductID: item.ProductID,
			}); err != nil {
				h.rollbackOrderCreate(ctx, order.ID, userID, originalItems, createdBookProductIDs)
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to finalize purchased books"})
			}
			createdBookProductIDs = append(createdBookProductIDs, item.ProductID)
		}
	}

	return c.Status(fiber.StatusCreated).JSON(order)
}

func (h *OrderHandler) rollbackOrderCreate(
	ctx context.Context,
	orderID primitive.ObjectID,
	userID primitive.ObjectID,
	cartItems []models.CartItem,
	createdBookProductIDs []primitive.ObjectID,
) {
	for _, productID := range createdBookProductIDs {
		_ = h.userBookRepo.Delete(ctx, userID, productID)
	}
	_ = h.cartRepo.ReplaceItems(ctx, userID, cartItems)
	_ = h.orderRepo.Delete(ctx, orderID)
}

func (h *OrderHandler) validateDependencies() error {
	switch {
	case h.orderRepo == nil:
		return fmt.Errorf("order repository not configured")
	case h.cartRepo == nil:
		return fmt.Errorf("cart repository not configured")
	case h.mainProductRepo == nil:
		return fmt.Errorf("main product repository not configured")
	case h.addressRepo == nil:
		return fmt.Errorf("user address repository not configured")
	case h.userBookRepo == nil:
		return fmt.Errorf("user book repository not configured")
	default:
		return nil
	}
}

func (h *OrderHandler) GetMyOrders(c *fiber.Ctx) error {
	userID, err := h.currentUserID(c)
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
	orders, total, err := h.orderRepo.FindByUserID(ctx, userID, page, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to fetch orders"})
	}
	if orders == nil {
		orders = []models.Order{}
	}
	return c.JSON(fiber.Map{"data": orders, "total": total, "page": page, "limit": limit})
}

func (h *OrderHandler) GetOrderByID(c *fiber.Ctx) error {
	userID, err := h.currentUserID(c)
	if err != nil {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "invalid user"})
	}
	orderID, err := primitive.ObjectIDFromHex(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid order id"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	order, err := h.orderRepo.FindByID(ctx, orderID)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "order not found"})
		}
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "database error"})
	}
	if order.UserID != userID {
		return c.Status(fiber.StatusForbidden).JSON(fiber.Map{"error": "access denied"})
	}
	return c.JSON(order)
}
