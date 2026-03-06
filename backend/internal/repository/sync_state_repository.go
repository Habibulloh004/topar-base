package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SyncStateRepository struct {
	collection *mongo.Collection
}

func NewSyncStateRepository(db *mongo.Database) *SyncStateRepository {
	return &SyncStateRepository{collection: db.Collection("sync_states")}
}

func (r *SyncStateRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "key", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("key_unique"),
	})
	return err
}

func (r *SyncStateRepository) Get(ctx context.Context, key string) (*models.SyncState, error) {
	var state models.SyncState
	err := r.collection.FindOne(ctx, bson.M{"key": key}).Decode(&state)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return &state, nil
}

func (r *SyncStateRepository) Upsert(ctx context.Context, key string, nextURL string, completed bool) error {
	_, err := r.collection.UpdateOne(
		ctx,
		bson.M{"key": key},
		bson.M{"$set": bson.M{
			"key":       key,
			"nextUrl":   nextURL,
			"completed": completed,
			"updatedAt": time.Now().UTC(),
		}},
		options.Update().SetUpsert(true),
	)
	return err
}

func (r *SyncStateRepository) Reset(ctx context.Context, key string) error {
	_, err := r.collection.DeleteOne(ctx, bson.M{"key": key})
	return err
}
