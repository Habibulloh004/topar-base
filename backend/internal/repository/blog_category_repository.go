package repository

import (
	"context"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BlogCategoryRepository struct {
	collection *mongo.Collection
}

func NewBlogCategoryRepository(db *mongo.Database) *BlogCategoryRepository {
	return &BlogCategoryRepository{collection: db.Collection("blog_categories")}
}

func (r *BlogCategoryRepository) List(ctx context.Context) ([]models.BlogCategory, error) {
	opts := options.Find().SetSort(bson.D{{Key: "name", Value: 1}})
	cursor, err := r.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var cats []models.BlogCategory
	if err := cursor.All(ctx, &cats); err != nil {
		return nil, err
	}
	return cats, nil
}

func (r *BlogCategoryRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*models.BlogCategory, error) {
	var cat models.BlogCategory
	err := r.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&cat)
	if err != nil {
		return nil, err
	}
	return &cat, nil
}

func (r *BlogCategoryRepository) FindBySlug(ctx context.Context, slug string) (*models.BlogCategory, error) {
	var cat models.BlogCategory
	err := r.collection.FindOne(ctx, bson.M{"slug": slug}).Decode(&cat)
	if err != nil {
		return nil, err
	}
	return &cat, nil
}
