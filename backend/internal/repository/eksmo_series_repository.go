package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EksmoSeriesRepository struct {
	collection *mongo.Collection
}

func NewEksmoSeriesRepository(db *mongo.Database) *EksmoSeriesRepository {
	return &EksmoSeriesRepository{collection: db.Collection("eksmo_series")}
}

func (r *EksmoSeriesRepository) EnsureIndexes(ctx context.Context) error {
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
			Keys:    bson.D{{Key: "organizationGuid", Value: 1}},
			Options: options.Index().SetName("organizationGuid_idx"),
		},
	}
	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *EksmoSeriesRepository) UpsertBatch(ctx context.Context, seriesList []models.EksmoSeries) (upserted int, modified int, err error) {
	if len(seriesList) == 0 {
		return 0, 0, nil
	}

	operations := make([]mongo.WriteModel, 0, len(seriesList))
	seenGUIDs := make(map[string]struct{}, len(seriesList))
	now := time.Now().UTC()

	for _, s := range seriesList {
		if s.GUID == "" {
			continue
		}
		if _, exists := seenGUIDs[s.GUID]; exists {
			continue
		}
		seenGUIDs[s.GUID] = struct{}{}

		s.UpdatedAt = now
		operations = append(operations,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"guid": s.GUID}).
				SetUpdate(bson.M{"$set": s}).
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

func (r *EksmoSeriesRepository) List(ctx context.Context, page, limit int64, search string) ([]models.EksmoSeries, int64, error) {
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

	var seriesList []models.EksmoSeries
	if err := cursor.All(ctx, &seriesList); err != nil {
		return nil, 0, err
	}

	return seriesList, total, nil
}

func (r *EksmoSeriesRepository) GetByGUID(ctx context.Context, guid string) (*models.EksmoSeries, error) {
	var series models.EksmoSeries
	err := r.collection.FindOne(ctx, bson.M{"guid": guid}).Decode(&series)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &series, nil
}

func (r *EksmoSeriesRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}
