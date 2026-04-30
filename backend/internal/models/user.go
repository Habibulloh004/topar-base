package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type User struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	Email        string             `bson:"email" json:"email"`
	Phone        string             `bson:"phone,omitempty" json:"phone,omitempty"`
	PasswordHash string             `bson:"passwordHash" json:"-"`
	FirstName    string             `bson:"firstName" json:"firstName"`
	LastName     string             `bson:"lastName" json:"lastName"`
	DisplayName  string             `bson:"displayName" json:"displayName"`
	Avatar       string             `bson:"avatar,omitempty" json:"avatar,omitempty"`
	IsActive     bool               `bson:"isActive" json:"isActive"`
	CreatedAt    time.Time          `bson:"createdAt" json:"createdAt"`
	UpdatedAt    time.Time          `bson:"updatedAt" json:"updatedAt"`
}
