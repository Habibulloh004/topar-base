package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EksmoNicheRepository struct {
	collection *mongo.Collection
}

func NewEksmoNicheRepository(db *mongo.Database) *EksmoNicheRepository {
	return &EksmoNicheRepository{collection: db.Collection("eksmo_niches")}
}

func (r *EksmoNicheRepository) EnsureIndexes(ctx context.Context) error {
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
			Keys:    bson.D{{Key: "ownerGuid", Value: 1}},
			Options: options.Index().SetName("ownerGuid_idx"),
		},
	}
	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *EksmoNicheRepository) UpsertBatch(ctx context.Context, niches []models.EksmoNicheEntity) (upserted int, modified int, err error) {
	if len(niches) == 0 {
		return 0, 0, nil
	}

	operations := make([]mongo.WriteModel, 0, len(niches))
	seenGUIDs := make(map[string]struct{}, len(niches))
	now := time.Now().UTC()

	for _, n := range niches {
		if n.GUID == "" {
			continue
		}
		if _, exists := seenGUIDs[n.GUID]; exists {
			continue
		}
		seenGUIDs[n.GUID] = struct{}{}

		n.UpdatedAt = now
		operations = append(operations,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"guid": n.GUID}).
				SetUpdate(bson.M{
					"$set": bson.M{
						"name":      n.Name,
						"ownerGuid": n.OwnerGUID,
						"updatedAt": n.UpdatedAt,
					},
					"$setOnInsert": bson.M{
						"guid":     n.GUID,
						"syncedAt": now,
					},
				}).
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

func (r *EksmoNicheRepository) List(ctx context.Context, page, limit int64, search string) ([]models.EksmoNicheEntity, int64, error) {
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

	var niches []models.EksmoNicheEntity
	if err := cursor.All(ctx, &niches); err != nil {
		return nil, 0, err
	}

	return niches, total, nil
}

func (r *EksmoNicheRepository) GetByGUID(ctx context.Context, guid string) (*models.EksmoNicheEntity, error) {
	var niche models.EksmoNicheEntity
	err := r.collection.FindOne(ctx, bson.M{"guid": guid}).Decode(&niche)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &niche, nil
}

func (r *EksmoNicheRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}

// GetAll returns all niches without pagination
func (r *EksmoNicheRepository) GetAll(ctx context.Context) ([]models.EksmoNicheEntity, error) {
	cursor, err := r.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var niches []models.EksmoNicheEntity
	if err := cursor.All(ctx, &niches); err != nil {
		return nil, err
	}
	return niches, nil
}
