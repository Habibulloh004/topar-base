package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const (
	ParserRunStatusRunning  = "running"
	ParserRunStatusFinished = "finished"
	ParserRunStatusFailed   = "failed"
)

// ParserRun keeps metadata for one parse execution.
type ParserRun struct {
	ID               primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	SourceURL        string             `bson:"sourceUrl" json:"sourceUrl"`
	Limit            int                `bson:"limit" json:"limit"`
	Workers          int                `bson:"workers" json:"workers"`
	RequestsPerSec   float64            `bson:"requestsPerSec" json:"requestsPerSec"`
	DiscoveredURLs   int                `bson:"discoveredUrls" json:"discoveredUrls"`
	ParsedProducts   int                `bson:"parsedProducts" json:"parsedProducts"`
	RateLimitRetries int                `bson:"rateLimitRetries" json:"rateLimitRetries"`
	Status           string             `bson:"status" json:"status"`
	Error            string             `bson:"error,omitempty" json:"error,omitempty"`
	DetectedFields   []string           `bson:"detectedFields,omitempty" json:"detectedFields,omitempty"`
	CreatedAt        time.Time          `bson:"createdAt" json:"createdAt"`
	FinishedAt       *time.Time         `bson:"finishedAt,omitempty" json:"finishedAt,omitempty"`
}

// ParserRecord stores one parsed product per document.
type ParserRecord struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	RunID     primitive.ObjectID `bson:"runId" json:"runId"`
	SourceURL string             `bson:"sourceUrl" json:"sourceUrl"`
	Data      map[string]any     `bson:"data" json:"data"`
	CreatedAt time.Time          `bson:"createdAt" json:"createdAt"`
}

// ParserFieldRule maps a target field either from parsed source field or a constant.
type ParserFieldRule struct {
	Source   string `bson:"source,omitempty" json:"source,omitempty"`
	Constant string `bson:"constant,omitempty" json:"constant,omitempty"`
}

// ParserMappingProfile stores admin mapping presets.
type ParserMappingProfile struct {
	ID        primitive.ObjectID         `bson:"_id,omitempty" json:"id"`
	Name      string                     `bson:"name" json:"name"`
	Rules     map[string]ParserFieldRule `bson:"rules" json:"rules"`
	CreatedAt time.Time                  `bson:"createdAt" json:"createdAt"`
	UpdatedAt time.Time                  `bson:"updatedAt" json:"updatedAt"`
}

type ParserSchemaField struct {
	Key         string `json:"key"`
	Description string `json:"description,omitempty"`
}

type ParserTargetSchema struct {
	Eksmo []ParserSchemaField `json:"eksmo"`
	Main  []ParserSchemaField `json:"main"`
}

type ParserSyncResult struct {
	RunID            primitive.ObjectID `json:"runId"`
	EksmoUpserted    int                `json:"eksmoUpserted"`
	EksmoModified    int                `json:"eksmoModified"`
	EksmoSkipped     int                `json:"eksmoSkipped"`
	MainInserted     int                `json:"mainInserted"`
	MainModified     int                `json:"mainModified"`
	MainSkipped      int                `json:"mainSkipped"`
	InvalidCount     int                `json:"invalidCount"`
	TotalRecords     int                `json:"totalRecords"`
	MappingProfileID string             `json:"mappingProfileId,omitempty"`
	MappingProfile   string             `json:"mappingProfile,omitempty"`
}
