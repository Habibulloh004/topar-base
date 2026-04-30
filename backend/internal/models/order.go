package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type OrderItem struct {
	ProductID primitive.ObjectID `bson:"productId" json:"productId"`
	Name      string             `bson:"name" json:"name"`
	Price     float64            `bson:"price" json:"price"`
	Quantity  int                `bson:"quantity" json:"quantity"`
}

type Order struct {
	ID                primitive.ObjectID  `bson:"_id,omitempty" json:"id"`
	UserID            primitive.ObjectID  `bson:"userId" json:"userId"`
	Items             []OrderItem         `bson:"items" json:"items"`
	TotalAmount       float64             `bson:"totalAmount" json:"totalAmount"`
	DeliveryAmount    float64             `bson:"deliveryAmount" json:"deliveryAmount"`
	BonusAmount       float64             `bson:"bonusAmount" json:"bonusAmount"`
	PaymentMethod     string              `bson:"paymentMethod" json:"paymentMethod"`
	DeliveryAddressID *primitive.ObjectID `bson:"deliveryAddressId,omitempty" json:"deliveryAddressId,omitempty"`
	Comments          string              `bson:"comments,omitempty" json:"comments,omitempty"`
	Status            string              `bson:"status" json:"status"` // pending, processing, shipped, delivered, cancelled
	CreatedAt         time.Time           `bson:"createdAt" json:"createdAt"`
	UpdatedAt         time.Time           `bson:"updatedAt" json:"updatedAt"`
}
