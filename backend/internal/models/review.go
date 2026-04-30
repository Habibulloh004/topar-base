package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Review struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID    primitive.ObjectID `bson:"userId" json:"userId"`
	ProductID primitive.ObjectID `bson:"productId" json:"productId"`
	Rating    int                `bson:"rating" json:"rating"` // 1–5
	Comment   string             `bson:"comment,omitempty" json:"comment,omitempty"`
	CreatedAt time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt time.Time          `bson:"updatedAt" json:"updatedAt"`
}
