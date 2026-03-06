package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Embedded structs for nested API objects

type EksmoSubject struct {
	GUID      string `bson:"guid,omitempty" json:"guid,omitempty"`
	Name      string `bson:"name" json:"name"`
	OwnerGUID string `bson:"ownerGuid,omitempty" json:"ownerGuid,omitempty"` // Points to parent Niche
}

type EksmoNiche struct {
	GUID      string `bson:"guid,omitempty" json:"guid,omitempty"`
	Name      string `bson:"name" json:"name"`
	OwnerGUID string `bson:"ownerGuid,omitempty" json:"ownerGuid,omitempty"` // Points to parent Niche
}

type EksmoBrand struct {
	GUID string `bson:"guid,omitempty" json:"guid,omitempty"`
	Name string `bson:"name" json:"name"`
}

type EksmoProductAuthorRef struct {
	GUID         string `bson:"guid" json:"guid"`
	Code         string `bson:"code,omitempty" json:"code,omitempty"`
	Name         string `bson:"name" json:"name"`
	IsWriter     bool   `bson:"isWriter" json:"isWriter"`
	IsTranslator bool   `bson:"isTranslator" json:"isTranslator"`
	IsArtist     bool   `bson:"isArtist" json:"isArtist"`
}

type EksmoProductTagRef struct {
	GUID string `bson:"guid" json:"guid"`
	Name string `bson:"name" json:"name"`
}

type EksmoProductGenreRef struct {
	GUID string `bson:"guid,omitempty" json:"guid,omitempty"`
	Name string `bson:"name" json:"name"`
}

type EksmoProductSeriesRef struct {
	GUID string `bson:"guid,omitempty" json:"guid,omitempty"`
	Name string `bson:"name" json:"name"`
}

type EksmoProductPublisherRef struct {
	GUID string `bson:"guid,omitempty" json:"guid,omitempty"`
	Name string `bson:"name" json:"name"`
}

type EksmoProduct struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id"`

	// Primary identifiers
	GUID    string `bson:"guid,omitempty" json:"guid,omitempty"`
	GUIDNOM string `bson:"guidNom,omitempty" json:"guidNom,omitempty"`
	NomCode string `bson:"nomcode,omitempty" json:"nomcode,omitempty"`
	ISBN    string `bson:"isbn,omitempty" json:"isbn,omitempty"`

	// Basic info
	Name        string `bson:"name" json:"name"`
	AuthorCover string `bson:"authorCover,omitempty" json:"authorCover,omitempty"`
	Annotation  string `bson:"annotation,omitempty" json:"annotation,omitempty"`

	// Categorization - inline objects
	Subject *EksmoSubject `bson:"subject,omitempty" json:"subject,omitempty"`
	Niche   *EksmoNiche   `bson:"niche,omitempty" json:"niche,omitempty"`
	Brand   *EksmoBrand   `bson:"brand,omitempty" json:"brand,omitempty"`

	// References with embedded data for filtering
	AuthorRefs []EksmoProductAuthorRef `bson:"authorRefs,omitempty" json:"authorRefs,omitempty"`
	TagRefs    []EksmoProductTagRef    `bson:"tagRefs,omitempty" json:"tagRefs,omitempty"`
	GenreRefs  []EksmoProductGenreRef  `bson:"genreRefs,omitempty" json:"genreRefs,omitempty"`

	// Series reference
	Series *EksmoProductSeriesRef `bson:"series,omitempty" json:"series,omitempty"`

	// Publisher reference
	Publisher *EksmoProductPublisherRef `bson:"publisherRef,omitempty" json:"publisherRef,omitempty"`

	// Physical attributes
	Pages          int    `bson:"pages,omitempty" json:"pages,omitempty"`
	Format         string `bson:"format,omitempty" json:"format,omitempty"`
	PaperType      string `bson:"paperType,omitempty" json:"paperType,omitempty"`
	BindingType    string `bson:"bindingType,omitempty" json:"bindingType,omitempty"`
	AgeRestriction string `bson:"ageRestriction,omitempty" json:"ageRestriction,omitempty"`

	// Covers
	CoverURL string            `bson:"coverUrl,omitempty" json:"coverUrl,omitempty"`
	Covers   map[string]string `bson:"covers,omitempty" json:"covers,omitempty"`

	// Category linking (to main_categories collection)
	CategoryIDs  []primitive.ObjectID `bson:"categoryIds,omitempty" json:"categoryIds,omitempty"`
	CategoryPath []string             `bson:"categoryPath,omitempty" json:"categoryPath,omitempty"`

	// Search optimization - denormalized arrays for text search and filtering
	AuthorNames []string `bson:"authorNames,omitempty" json:"authorNames,omitempty"`
	TagNames    []string `bson:"tagNames,omitempty" json:"tagNames,omitempty"`
	GenreNames  []string `bson:"genreNames,omitempty" json:"genreNames,omitempty"`

	// Legacy fields for backward compatibility
	SubjectName   string `bson:"subjectName,omitempty" json:"subjectName,omitempty"`
	SerieName     string `bson:"serieName,omitempty" json:"serieName,omitempty"`
	BrandName     string `bson:"brandName,omitempty" json:"brandName,omitempty"`
	PublisherName string `bson:"publisher,omitempty" json:"publisher,omitempty"`

	// UI helper flag: whether this Eksmo product already exists in main_products.
	InMainProducts bool `bson:"inMainProducts,omitempty" json:"inMainProducts,omitempty"`

	// Raw API response for future fields
	Raw       bson.M    `bson:"raw,omitempty" json:"-"`
	SyncedAt  time.Time `bson:"syncedAt" json:"syncedAt"`
	UpdatedAt time.Time `bson:"updatedAt" json:"updatedAt"`
}

type EksmoSyncResult struct {
	Message      string `json:"message"`
	Collection   string `json:"collection"`
	Pages        int    `json:"pages"`
	Fetched      int    `json:"fetched"`
	Upserted     int    `json:"upserted"`
	Modified     int    `json:"modified"`
	Skipped      int    `json:"skipped"`
	TotalInAPI   int    `json:"totalInApi"`
	StoppedEarly bool   `json:"stoppedEarly"`
	NextURL      string `json:"nextUrl,omitempty"`
	Completed    bool   `json:"completed"`
}

type EksmoMeta struct {
	Subjects   []string          `json:"subjects"`
	Brands     []string          `json:"brands"`
	Series     []string          `json:"series"`
	Publishers []string          `json:"publishers"`
	Authors    []EksmoAuthorMeta `json:"authors,omitempty"`
	Tags       []string          `json:"tags,omitempty"`
	Genres     []string          `json:"genres,omitempty"`
}

type EksmoAuthorMeta struct {
	GUID string `json:"guid"`
	Name string `json:"name"`
}
