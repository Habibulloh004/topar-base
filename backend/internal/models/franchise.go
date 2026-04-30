package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type WorkHours struct {
	Open  string `bson:"open" json:"open"`
	Close string `bson:"close" json:"close"`
}

type Franchise struct {
	ID            primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Name          string             `bson:"name" json:"name"`
	Description   string             `bson:"description,omitempty" json:"description,omitempty"`
	Image         string             `bson:"image,omitempty" json:"image,omitempty"`
	City          string             `bson:"city" json:"city"`
	District      string             `bson:"district,omitempty" json:"district,omitempty"`
	Address       string             `bson:"address" json:"address"`
	Phone         string             `bson:"phone,omitempty" json:"phone,omitempty"`
	Weekdays      WorkHours          `bson:"weekdays" json:"weekdays"`
	Weekend       WorkHours          `bson:"weekend" json:"weekend"`
	WeekendClosed bool               `bson:"weekendClosed" json:"weekendClosed"`
	Latitude      float64            `bson:"latitude,omitempty" json:"latitude,omitempty"`
	Longitude     float64            `bson:"longitude,omitempty" json:"longitude,omitempty"`
	IsActive      bool               `bson:"isActive" json:"isActive"`
	CreatedAt     time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt     time.Time          `bson:"updatedAt" json:"updatedAt"`
}
