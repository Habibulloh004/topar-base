package models

import "go.mongodb.org/mongo-driver/bson/primitive"

type Category struct {
	ID         primitive.ObjectID   `bson:"_id,omitempty" json:"id"`
	Name       string               `bson:"name" json:"name"`
	ParentID   string               `bson:"parentId" json:"parentId"`
	ProductIDs []primitive.ObjectID `bson:"productIds,omitempty" json:"productIds,omitempty"`
}

type CategoryInput struct {
	Name     string          `json:"name"`
	Children []CategoryInput `json:"children,omitempty"`
}

type CategoryNode struct {
	ID       string         `json:"id"`
	Name     string         `json:"name"`
	ParentID string         `json:"parentId"`
	Children []CategoryNode `json:"children,omitempty"`
}
