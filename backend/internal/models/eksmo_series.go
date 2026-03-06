package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type EksmoSeries struct {
	ID                primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	GUID              string             `bson:"guid" json:"guid"`
	Code              string             `bson:"code,omitempty" json:"code,omitempty"`
	Name              string             `bson:"name" json:"name"`
	Description       string             `bson:"description,omitempty" json:"description,omitempty"`
	OrganizationGUID  string             `bson:"organizationGuid,omitempty" json:"organizationGuid,omitempty"`
	OrganizationName  string             `bson:"organizationName,omitempty" json:"organizationName,omitempty"`
	SyncedAt          time.Time          `bson:"syncedAt" json:"syncedAt"`
	UpdatedAt         time.Time          `bson:"updatedAt" json:"updatedAt"`
}

type EksmoSeriesSyncResult struct {
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
