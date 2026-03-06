package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EksmoSubjectRepository struct {
	collection *mongo.Collection
}

func NewEksmoSubjectRepository(db *mongo.Database) *EksmoSubjectRepository {
	return &EksmoSubjectRepository{collection: db.Collection("eksmo_subjects")}
}

func (r *EksmoSubjectRepository) EnsureIndexes(ctx context.Context) error {
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

func (r *EksmoSubjectRepository) UpsertBatch(ctx context.Context, subjects []models.EksmoSubjectEntity) (upserted int, modified int, err error) {
	if len(subjects) == 0 {
		return 0, 0, nil
	}

	operations := make([]mongo.WriteModel, 0, len(subjects))
	seenGUIDs := make(map[string]struct{}, len(subjects))
	now := time.Now().UTC()

	for _, s := range subjects {
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
				SetUpdate(bson.M{
					"$set": bson.M{
						"name":      s.Name,
						"ownerGuid": s.OwnerGUID,
						"updatedAt": s.UpdatedAt,
					},
					"$setOnInsert": bson.M{
						"guid":     s.GUID,
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

func (r *EksmoSubjectRepository) List(ctx context.Context, page, limit int64, search string) ([]models.EksmoSubjectEntity, int64, error) {
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

	var subjects []models.EksmoSubjectEntity
	if err := cursor.All(ctx, &subjects); err != nil {
		return nil, 0, err
	}

	return subjects, total, nil
}

func (r *EksmoSubjectRepository) GetByGUID(ctx context.Context, guid string) (*models.EksmoSubjectEntity, error) {
	var subject models.EksmoSubjectEntity
	err := r.collection.FindOne(ctx, bson.M{"guid": guid}).Decode(&subject)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &subject, nil
}

func (r *EksmoSubjectRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}

// GetAll returns all subjects without pagination
func (r *EksmoSubjectRepository) GetAll(ctx context.Context) ([]models.EksmoSubjectEntity, error) {
	cursor, err := r.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var subjects []models.EksmoSubjectEntity
	if err := cursor.All(ctx, &subjects); err != nil {
		return nil, err
	}
	return subjects, nil
}
