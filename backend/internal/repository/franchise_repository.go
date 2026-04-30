package repository

import (
	"context"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type FranchiseRepository struct {
	collection *mongo.Collection
}

func NewFranchiseRepository(db *mongo.Database) *FranchiseRepository {
	return &FranchiseRepository{collection: db.Collection("franchises")}
}

func (r *FranchiseRepository) List(ctx context.Context, city, district string) ([]models.Franchise, error) {
	filter := bson.M{"isActive": true}
	if city != "" {
		filter["city"] = city
	}
	if district != "" {
		filter["district"] = district
	}
	opts := options.Find().SetSort(bson.D{{Key: "name", Value: 1}})
	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var franchises []models.Franchise
	if err := cursor.All(ctx, &franchises); err != nil {
		return nil, err
	}
	return franchises, nil
}

func (r *FranchiseRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*models.Franchise, error) {
	var f models.Franchise
	err := r.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&f)
	if err != nil {
		return nil, err
	}
	return &f, nil
}

func (r *FranchiseRepository) DistinctCities(ctx context.Context) ([]string, error) {
	vals, err := r.collection.Distinct(ctx, "city", bson.M{"isActive": true})
	if err != nil {
		return nil, err
	}
	cities := make([]string, 0, len(vals))
	for _, v := range vals {
		if s, ok := v.(string); ok {
			cities = append(cities, s)
		}
	}
	return cities, nil
}
