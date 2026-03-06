package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// EksmoSubjectEntity represents a subject extracted from product data
// Subjects are linked to Niches via OwnerGUID
type EksmoSubjectEntity struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	GUID      string             `bson:"guid" json:"guid"`
	Name      string             `bson:"name" json:"name"`
	OwnerGUID string             `bson:"ownerGuid" json:"ownerGuid"` // Points to parent Niche GUID
	SyncedAt  time.Time          `bson:"syncedAt" json:"syncedAt"`
	UpdatedAt time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// EksmoSubjectSyncResult holds the result of subject extraction
type EksmoSubjectSyncResult struct {
	Collection string `json:"collection"`
	Upserted   int    `json:"upserted"`
	Modified   int    `json:"modified"`
	Total      int64  `json:"total"`
}
