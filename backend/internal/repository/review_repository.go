package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ReviewRepository struct {
	collection *mongo.Collection
}

func NewReviewRepository(db *mongo.Database) *ReviewRepository {
	return &ReviewRepository{collection: db.Collection("reviews")}
}

func (r *ReviewRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "userId", Value: 1}, {Key: "productId", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("user_product_unique"),
	})
	return err
}

func (r *ReviewRepository) FindByProductID(ctx context.Context, productID primitive.ObjectID) ([]models.Review, error) {
	opts := options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}})
	cursor, err := r.collection.Find(ctx, bson.M{"productId": productID}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var reviews []models.Review
	if err := cursor.All(ctx, &reviews); err != nil {
		return nil, err
	}
	return reviews, nil
}

func (r *ReviewRepository) FindByUserAndProduct(ctx context.Context, userID, productID primitive.ObjectID) (*models.Review, error) {
	var review models.Review
	err := r.collection.FindOne(ctx, bson.M{"userId": userID, "productId": productID}).Decode(&review)
	if err != nil {
		return nil, err
	}
	return &review, nil
}

func (r *ReviewRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*models.Review, error) {
	var review models.Review
	err := r.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&review)
	if err != nil {
		return nil, err
	}
	return &review, nil
}

func (r *ReviewRepository) Create(ctx context.Context, review *models.Review) error {
	review.ID = primitive.NewObjectID()
	review.CreatedAt = time.Now()
	review.UpdatedAt = time.Now()
	_, err := r.collection.InsertOne(ctx, review)
	return err
}

func (r *ReviewRepository) Update(ctx context.Context, id primitive.ObjectID, fields bson.M) error {
	fields["updatedAt"] = time.Now()
	_, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": fields})
	return err
}

func (r *ReviewRepository) Delete(ctx context.Context, id primitive.ObjectID) error {
	_, err := r.collection.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

func (r *ReviewRepository) AverageRating(ctx context.Context, productID primitive.ObjectID) (float64, int, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"productId": productID}}},
		{{Key: "$group", Value: bson.M{
			"_id":   nil,
			"avg":   bson.M{"$avg": "$rating"},
			"count": bson.M{"$sum": 1},
		}}},
	}
	cursor, err := r.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return 0, 0, err
	}
	defer cursor.Close(ctx)
	var result []struct {
		Avg   float64 `bson:"avg"`
		Count int     `bson:"count"`
	}
	if err := cursor.All(ctx, &result); err != nil {
		return 0, 0, err
	}
	if len(result) == 0 {
		return 0, 0, nil
	}
	return result[0].Avg, result[0].Count, nil
}
