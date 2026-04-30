package repository

import (
	"context"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type BlogPostRepository struct {
	collection *mongo.Collection
}

func NewBlogPostRepository(db *mongo.Database) *BlogPostRepository {
	return &BlogPostRepository{collection: db.Collection("blog_posts")}
}

func (r *BlogPostRepository) List(ctx context.Context, categoryID *primitive.ObjectID, page, limit int) ([]models.BlogPost, int64, error) {
	filter := bson.M{"isPublished": true}
	if categoryID != nil {
		filter["categoryId"] = *categoryID
	}
	total, err := r.collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	skip := int64((page - 1) * limit)
	opts := options.Find().
		SetSort(bson.D{{Key: "createdAt", Value: -1}}).
		SetSkip(skip).
		SetLimit(int64(limit))
	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)
	var posts []models.BlogPost
	if err := cursor.All(ctx, &posts); err != nil {
		return nil, 0, err
	}
	return posts, total, nil
}

func (r *BlogPostRepository) ListPopular(ctx context.Context, limit int) ([]models.BlogPost, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "viewCount", Value: -1}}).
		SetLimit(int64(limit)).
		SetProjection(bson.M{"content": 0})
	cursor, err := r.collection.Find(ctx, bson.M{"isPublished": true}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var posts []models.BlogPost
	if err := cursor.All(ctx, &posts); err != nil {
		return nil, err
	}
	return posts, nil
}

func (r *BlogPostRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*models.BlogPost, error) {
	var post models.BlogPost
	err := r.collection.FindOne(ctx, bson.M{"_id": id, "isPublished": true}).Decode(&post)
	if err != nil {
		return nil, err
	}
	return &post, nil
}

func (r *BlogPostRepository) IncrementViews(ctx context.Context, id primitive.ObjectID) error {
	_, err := r.collection.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$inc": bson.M{"viewCount": 1}},
	)
	return err
}
