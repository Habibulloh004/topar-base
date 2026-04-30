package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type GiftCertificate struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Amount      float64            `bson:"amount" json:"amount"`
	Description string             `bson:"description,omitempty" json:"description,omitempty"`
	Image       string             `bson:"image,omitempty" json:"image,omitempty"`
	IsActive    bool               `bson:"isActive" json:"isActive"`
	SortOrder   int                `bson:"sortOrder" json:"sortOrder"`
	CreatedAt   time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt   time.Time          `bson:"updatedAt" json:"updatedAt"`
}
