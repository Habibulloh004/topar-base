package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type EksmoAuthor struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	GUID         string             `bson:"guid" json:"guid"`
	Code         string             `bson:"code,omitempty" json:"code,omitempty"`
	Name         string             `bson:"name" json:"name"`
	FirstName    string             `bson:"firstName,omitempty" json:"firstName,omitempty"`
	Surname      string             `bson:"surname,omitempty" json:"surname,omitempty"`
	SecondName   string             `bson:"secondName,omitempty" json:"secondName,omitempty"`
	IsWriter     bool               `bson:"isWriter" json:"isWriter"`
	IsTranslator bool               `bson:"isTranslator" json:"isTranslator"`
	IsArtist     bool               `bson:"isArtist" json:"isArtist"`
	IsSpeaker    bool               `bson:"isSpeaker" json:"isSpeaker"`
	IsRedactor   bool               `bson:"isRedactor" json:"isRedactor"`
	IsCompiler   bool               `bson:"isCompiler" json:"isCompiler"`
	DateBirth    string             `bson:"dateBirth,omitempty" json:"dateBirth,omitempty"`
	DateDeath    string             `bson:"dateDeath,omitempty" json:"dateDeath,omitempty"`
	SyncedAt     time.Time          `bson:"syncedAt" json:"syncedAt"`
	UpdatedAt    time.Time          `bson:"updatedAt" json:"updatedAt"`
}

type EksmoAuthorSyncResult struct {
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
