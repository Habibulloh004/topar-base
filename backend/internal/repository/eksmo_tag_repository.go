package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EksmoTagRepository struct {
	collection *mongo.Collection
}

func NewEksmoTagRepository(db *mongo.Database) *EksmoTagRepository {
	return &EksmoTagRepository{collection: db.Collection("eksmo_tags")}
}

func (r *EksmoTagRepository) EnsureIndexes(ctx context.Context) error {
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
			Keys:    bson.D{{Key: "isActive", Value: 1}},
			Options: options.Index().SetName("isActive_idx"),
		},
	}
	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *EksmoTagRepository) UpsertBatch(ctx context.Context, tags []models.EksmoTag) (upserted int, modified int, err error) {
	if len(tags) == 0 {
		return 0, 0, nil
	}

	operations := make([]mongo.WriteModel, 0, len(tags))
	seenGUIDs := make(map[string]struct{}, len(tags))
	now := time.Now().UTC()

	for _, t := range tags {
		if t.GUID == "" {
			continue
		}
		if _, exists := seenGUIDs[t.GUID]; exists {
			continue
		}
		seenGUIDs[t.GUID] = struct{}{}

		t.UpdatedAt = now
		operations = append(operations,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"guid": t.GUID}).
				SetUpdate(bson.M{"$set": t}).
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

func (r *EksmoTagRepository) List(ctx context.Context, page, limit int64, search string, activeOnly bool) ([]models.EksmoTag, int64, error) {
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
	if activeOnly {
		filter["isActive"] = true
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

	var tags []models.EksmoTag
	if err := cursor.All(ctx, &tags); err != nil {
		return nil, 0, err
	}

	return tags, total, nil
}

func (r *EksmoTagRepository) GetByGUID(ctx context.Context, guid string) (*models.EksmoTag, error) {
	var tag models.EksmoTag
	err := r.collection.FindOne(ctx, bson.M{"guid": guid}).Decode(&tag)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &tag, nil
}

func (r *EksmoTagRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}
