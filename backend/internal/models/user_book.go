package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UserBook struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID      primitive.ObjectID `bson:"userId" json:"userId"`
	ProductID   primitive.ObjectID `bson:"productId" json:"productId"`
	PurchasedAt time.Time          `bson:"purchasedAt" json:"purchasedAt"`
}
