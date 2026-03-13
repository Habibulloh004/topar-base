package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// InvalidProduct keeps raw sync records that could not be converted into a valid product.
type InvalidProduct struct {
	ID         primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	RunID      primitive.ObjectID `bson:"runId,omitempty" json:"runId,omitempty"`
	SourceURL  string             `bson:"sourceUrl,omitempty" json:"sourceUrl,omitempty"`
	SyncSource string             `bson:"syncSource" json:"syncSource"`
	Error      string             `bson:"error" json:"error"`
	Payload    any                `bson:"payload,omitempty" json:"payload,omitempty"`
	CreatedAt  time.Time          `bson:"createdAt" json:"createdAt"`
}
