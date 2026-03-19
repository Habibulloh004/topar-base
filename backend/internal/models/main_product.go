package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// MainProduct stores products copied from Eksmo into local main catalog.
// Each product belongs to exactly one main category.
type MainProduct struct {
	ID primitive.ObjectID `bson:"_id,omitempty" json:"id"`

	SourceProductID primitive.ObjectID `bson:"sourceProductId,omitempty" json:"sourceProductId,omitempty"`
	SourceGUID      string             `bson:"sourceGuid,omitempty" json:"sourceGuid,omitempty"`
	SourceGUIDNOM   string             `bson:"sourceGuidNom,omitempty" json:"sourceGuidNom,omitempty"`
	SourceNomCode   string             `bson:"sourceNomcode,omitempty" json:"sourceNomcode,omitempty"`
	ISBN            string             `bson:"isbn,omitempty" json:"isbn,omitempty"`

	Name           string            `bson:"name" json:"name"`
	AuthorCover    string            `bson:"authorCover,omitempty" json:"authorCover,omitempty"`
	AuthorNames    []string          `bson:"authorNames,omitempty" json:"authorNames,omitempty"`
	AuthorRefs     []EksmoProductAuthorRef `bson:"authorRefs,omitempty" json:"authorRefs,omitempty"`
	TagRefs        []EksmoProductTagRef    `bson:"tagRefs,omitempty" json:"tagRefs,omitempty"`
	GenreRefs      []EksmoProductGenreRef  `bson:"genreRefs,omitempty" json:"genreRefs,omitempty"`
	TagNames       []string                `bson:"tagNames,omitempty" json:"tagNames,omitempty"`
	GenreNames     []string                `bson:"genreNames,omitempty" json:"genreNames,omitempty"`
	Annotation     string            `bson:"annotation,omitempty" json:"annotation,omitempty"`
	CoverURL       string            `bson:"coverUrl,omitempty" json:"coverUrl,omitempty"`
	Covers         map[string]string `bson:"covers,omitempty" json:"covers,omitempty"`
	Pages          int               `bson:"pages,omitempty" json:"pages,omitempty"`
	Format         string            `bson:"format,omitempty" json:"format,omitempty"`
	PaperType      string            `bson:"paperType,omitempty" json:"paperType,omitempty"`
	BindingType    string            `bson:"bindingType,omitempty" json:"bindingType,omitempty"`
	AgeRestriction string            `bson:"ageRestriction,omitempty" json:"ageRestriction,omitempty"`

	SubjectName   string `bson:"subjectName,omitempty" json:"subjectName,omitempty"`
	NicheName     string `bson:"nicheName,omitempty" json:"nicheName,omitempty"`
	BrandName     string `bson:"brandName,omitempty" json:"brandName,omitempty"`
	SeriesName    string `bson:"seriesName,omitempty" json:"seriesName,omitempty"`
	PublisherName string `bson:"publisherName,omitempty" json:"publisherName,omitempty"`

	ISBNNormalized string     `bson:"isbnNormalized,omitempty" json:"-"`
	Quantity       float64    `bson:"quantity,omitempty" json:"quantity,omitempty"`
	Price          float64    `bson:"price,omitempty" json:"price,omitempty"`
	BillzUpdatedAt *time.Time `bson:"billzUpdatedAt,omitempty" json:"billzUpdatedAt,omitempty"`

	CategoryID   primitive.ObjectID `bson:"categoryId,omitempty" json:"categoryId,omitempty"`
	CategoryPath []string           `bson:"categoryPath,omitempty" json:"categoryPath,omitempty"`

	CreatedAt time.Time `bson:"createdAt,omitempty" json:"createdAt,omitempty"`
	UpdatedAt time.Time `bson:"updatedAt" json:"updatedAt"`
}
