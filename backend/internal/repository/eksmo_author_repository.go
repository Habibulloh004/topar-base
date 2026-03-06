package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EksmoAuthorRepository struct {
	collection *mongo.Collection
}

func NewEksmoAuthorRepository(db *mongo.Database) *EksmoAuthorRepository {
	return &EksmoAuthorRepository{collection: db.Collection("eksmo_authors")}
}

func (r *EksmoAuthorRepository) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "guid", Value: 1}},
			Options: options.Index().SetName("guid_unique").SetUnique(true).SetSparse(true),
		},
		{
			Keys:    bson.D{{Key: "name", Value: 1}},
			Options: options.Index().SetName("name_idx"),
		},
		{
			Keys:    bson.D{{Key: "isWriter", Value: 1}},
			Options: options.Index().SetName("isWriter_idx"),
		},
		{
			Keys:    bson.D{{Key: "updatedAt", Value: -1}},
			Options: options.Index().SetName("updatedAt_desc_idx"),
		},
	}
	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *EksmoAuthorRepository) UpsertBatch(ctx context.Context, authors []models.EksmoAuthor) (upserted int, modified int, err error) {
	if len(authors) == 0 {
		return 0, 0, nil
	}

	operations := make([]mongo.WriteModel, 0, len(authors))
	seenGUIDs := make(map[string]struct{}, len(authors))
	now := time.Now().UTC()

	for _, a := range authors {
		if a.GUID == "" {
			continue
		}
		if _, exists := seenGUIDs[a.GUID]; exists {
			continue
		}
		seenGUIDs[a.GUID] = struct{}{}

		a.UpdatedAt = now
		operations = append(operations,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"guid": a.GUID}).
				SetUpdate(bson.M{"$set": a}).
				SetUpsert(true))
	}

	if len(operations) == 0 {
		return 0, 0, nil
	}

	result, err := r.collection.BulkWrite(ctx, operations, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, 0, err
	}

	return int(result.UpsertedCount), int(result.ModifiedCount), nil
}

func (r *EksmoAuthorRepository) List(ctx context.Context, page, limit int64, search string, writerOnly bool) ([]models.EksmoAuthor, int64, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 20
	}

	filter := bson.M{}
	if search != "" {
		filter["name"] = bson.M{"$regex": search, "$options": "i"}
	}
	if writerOnly {
		filter["isWriter"] = true
	}

	total, err := r.collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	opts := options.Find().
		SetSkip((page - 1) * limit).
		SetLimit(limit).
		SetSort(bson.D{{Key: "name", Value: 1}})

	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	var authors []models.EksmoAuthor
	if err := cursor.All(ctx, &authors); err != nil {
		return nil, 0, err
	}

	return authors, total, nil
}

func (r *EksmoAuthorRepository) GetByGUID(ctx context.Context, guid string) (*models.EksmoAuthor, error) {
	var author models.EksmoAuthor
	err := r.collection.FindOne(ctx, bson.M{"guid": guid}).Decode(&author)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &author, nil
}

func (r *EksmoAuthorRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}
