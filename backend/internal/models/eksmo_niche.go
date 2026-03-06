package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// EksmoNicheEntity represents a niche (category level) extracted from product data
// Niches form a hierarchy via OwnerGUID pointing to parent niches
type EksmoNicheEntity struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id,omitempty"`
	GUID      string             `bson:"guid" json:"guid"`
	Name      string             `bson:"name" json:"name"`
	OwnerGUID string             `bson:"ownerGuid" json:"ownerGuid"` // Points to parent Niche GUID
	SyncedAt  time.Time          `bson:"syncedAt" json:"syncedAt"`
	UpdatedAt time.Time          `bson:"updatedAt" json:"updatedAt"`
}

// EksmoNicheSyncResult holds the result of niche extraction
type EksmoNicheSyncResult struct {
	Collection string `json:"collection"`
	Upserted   int    `json:"upserted"`
	Modified   int    `json:"modified"`
	Total      int64  `json:"total"`
}

// EksmoSubjectRef is a reference to a subject used in tree nodes
type EksmoSubjectRef struct {
	GUID string `json:"guid"`
	Name string `json:"name"`
}

// EksmoNicheNode represents a niche in the hierarchical tree view
type EksmoNicheNode struct {
	GUID      string             `json:"guid"`
	Name      string             `json:"name"`
	Type      string             `json:"type"` // "niche" or "subject"
	Children  []EksmoNicheNode   `json:"children,omitempty"`
	Subjects  []EksmoSubjectRef  `json:"subjects,omitempty"`
}
