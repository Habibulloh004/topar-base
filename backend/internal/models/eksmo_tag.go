package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type EksmoTag struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	GUID         string             `bson:"guid" json:"guid"`
	Code         string             `bson:"code,omitempty" json:"code,omitempty"`
	Name         string             `bson:"name" json:"name"`
	CategoryGUID string             `bson:"categoryGuid,omitempty" json:"categoryGuid,omitempty"`
	IsActive     bool               `bson:"isActive" json:"isActive"`
	SyncedAt     time.Time          `bson:"syncedAt" json:"syncedAt"`
	UpdatedAt    time.Time          `bson:"updatedAt" json:"updatedAt"`
}

type EksmoTagSyncResult struct {
	Message    string `json:"message"`
	Collection string `json:"collection"`
	Pages      int    `json:"pages"`
	Fetched    int    `json:"fetched"`
	Upserted   int    `json:"upserted"`
	Modified   int    `json:"modified"`
	TotalInAPI int    `json:"totalInApi"`
	Completed  bool   `json:"completed"`
	NextURL    string `json:"nextUrl,omitempty"`
}
