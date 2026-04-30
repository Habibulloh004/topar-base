package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BlogPost struct {
	ID            primitive.ObjectID  `bson:"_id,omitempty" json:"id"`
	Title         string              `bson:"title" json:"title"`
	Content       string              `bson:"content" json:"content"`
	Excerpt       string              `bson:"excerpt,omitempty" json:"excerpt,omitempty"`
	FeaturedImage string              `bson:"featuredImage,omitempty" json:"featuredImage,omitempty"`
	CategoryID    *primitive.ObjectID `bson:"categoryId,omitempty" json:"categoryId,omitempty"`
	AuthorName    string              `bson:"authorName,omitempty" json:"authorName,omitempty"`
	IsPublished   bool                `bson:"isPublished" json:"isPublished"`
	ViewCount     int                 `bson:"viewCount" json:"viewCount"`
	CreatedAt     time.Time           `bson:"createdAt" json:"createdAt"`
	UpdatedAt     time.Time           `bson:"updatedAt" json:"updatedAt"`
}
