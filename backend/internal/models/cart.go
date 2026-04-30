package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type CartItem struct {
	ProductID  primitive.ObjectID `bson:"productId" json:"productId"`
	Quantity   int                `bson:"quantity" json:"quantity"`
	PriceAtAdd float64            `bson:"priceAtAdd" json:"priceAtAdd"`
}

type Cart struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID    primitive.ObjectID `bson:"userId" json:"userId"`
	Items     []CartItem         `bson:"items" json:"items"`
	UpdatedAt time.Time          `bson:"updatedAt" json:"updatedAt"`
}
