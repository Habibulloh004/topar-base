package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type UserAddress struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID      primitive.ObjectID `bson:"userId" json:"userId"`
	Type        string             `bson:"type" json:"type"` // "home", "work", "other"
	City        string             `bson:"city" json:"city"`
	District    string             `bson:"district,omitempty" json:"district,omitempty"`
	AddressText string             `bson:"addressText" json:"addressText"`
	IsDefault   bool               `bson:"isDefault" json:"isDefault"`
	CreatedAt   time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt   time.Time          `bson:"updatedAt" json:"updatedAt"`
}
