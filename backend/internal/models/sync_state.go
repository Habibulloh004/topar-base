package models

import "time"

type SyncState struct {
	Key       string    `bson:"key" json:"key"`
	NextURL   string    `bson:"nextUrl,omitempty" json:"nextUrl,omitempty"`
	Completed bool      `bson:"completed" json:"completed"`
	UpdatedAt time.Time `bson:"updatedAt" json:"updatedAt"`
}
