package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EksmoPublisherRepository struct {
	collection *mongo.Collection
}

func NewEksmoPublisherRepository(db *mongo.Database) *EksmoPublisherRepository {
	return &EksmoPublisherRepository{collection: db.Collection("eksmo_publishers")}
}

func (r *EksmoPublisherRepository) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "guid", Value: 1}},
			Options: options.Index().SetName("guid_unique").SetUnique(true).SetSparse(true),
		},
		{
			Keys:    bson.D{{Key: "name", Value: 1}},
			Options: options.Index().SetName("name_idx"),
		},
	}
	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *EksmoPublisherRepository) UpsertBatch(ctx context.Context, publishers []models.EksmoPublisher) (upserted int, modified int, err error) {
	if len(publishers) == 0 {
		return 0, 0, nil
	}

	operations := make([]mongo.WriteModel, 0, len(publishers))
	seenGUIDs := make(map[string]struct{}, len(publishers))
	now := time.Now().UTC()

	for _, p := range publishers {
		if p.GUID == "" {
			continue
		}
		if _, exists := seenGUIDs[p.GUID]; exists {
			continue
		}
		seenGUIDs[p.GUID] = struct{}{}

		p.UpdatedAt = now
		operations = append(operations,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"guid": p.GUID}).
				SetUpdate(bson.M{"$set": p}).
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

func (r *EksmoPublisherRepository) List(ctx context.Context, page, limit int64, search string) ([]models.EksmoPublisher, int64, error) {
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

	var publishers []models.EksmoPublisher
	if err := cursor.All(ctx, &publishers); err != nil {
		return nil, 0, err
	}

	return publishers, total, nil
}

func (r *EksmoPublisherRepository) GetByGUID(ctx context.Context, guid string) (*models.EksmoPublisher, error) {
	var publisher models.EksmoPublisher
	err := r.collection.FindOne(ctx, bson.M{"guid": guid}).Decode(&publisher)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &publisher, nil
}

func (r *EksmoPublisherRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}
