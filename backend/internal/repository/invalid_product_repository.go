package repository

import (
	"context"
	"errors"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type InvalidProductRepository struct {
	collection *mongo.Collection
}

func NewInvalidProductRepository(db *mongo.Database) *InvalidProductRepository {
	return &InvalidProductRepository{collection: db.Collection("invalid_products")}
}

func (r *InvalidProductRepository) EnsureIndexes(ctx context.Context) error {
	if r == nil {
		return errors.New("invalid product repository is nil")
	}

	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "createdAt", Value: -1}},
			Options: options.Index().SetName("createdAt_desc_idx"),
		},
		{
			Keys:    bson.D{{Key: "runId", Value: 1}, {Key: "createdAt", Value: -1}},
			Options: options.Index().SetName("runId_createdAt_idx"),
		},
		{
			Keys:    bson.D{{Key: "syncSource", Value: 1}, {Key: "createdAt", Value: -1}},
			Options: options.Index().SetName("syncSource_createdAt_idx"),
		},
	}

	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *InvalidProductRepository) InsertMany(ctx context.Context, products []models.InvalidProduct) error {
	if r == nil {
		return errors.New("invalid product repository is nil")
	}
	if len(products) == 0 {
		return nil
	}

	now := time.Now().UTC()
	docs := make([]any, 0, len(products))
	for _, product := range products {
		if product.ID.IsZero() {
			product.ID = primitive.NewObjectID()
		}
		if product.CreatedAt.IsZero() {
			product.CreatedAt = now
		}
		docs = append(docs, product)
	}

	_, err := r.collection.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false))
	return err
}
