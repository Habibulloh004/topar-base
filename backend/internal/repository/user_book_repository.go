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

type UserBookRepository struct {
	collection *mongo.Collection
}

func NewUserBookRepository(db *mongo.Database) *UserBookRepository {
	return &UserBookRepository{collection: db.Collection("user_books")}
}

func (r *UserBookRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "userId", Value: 1}, {Key: "productId", Value: 1}},
		Options: options.Index().SetUnique(true).SetName("user_product_unique"),
	})
	return err
}

func (r *UserBookRepository) FindByUserID(ctx context.Context, userID primitive.ObjectID) ([]models.UserBook, error) {
	cursor, err := r.collection.Find(ctx, bson.M{"userId": userID})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var books []models.UserBook
	if err := cursor.All(ctx, &books); err != nil {
		return nil, err
	}
	return books, nil
}

func (r *UserBookRepository) Exists(ctx context.Context, userID, productID primitive.ObjectID) (bool, error) {
	count, err := r.collection.CountDocuments(ctx, bson.M{"userId": userID, "productId": productID})
	return count > 0, err
}

func (r *UserBookRepository) Create(ctx context.Context, ub *models.UserBook) error {
	ub.ID = primitive.NewObjectID()
	ub.PurchasedAt = time.Now()
	_, err := r.collection.InsertOne(ctx, ub)
	return err
}

func (r *UserBookRepository) Delete(ctx context.Context, userID, productID primitive.ObjectID) error {
	_, err := r.collection.DeleteOne(ctx, bson.M{"userId": userID, "productId": productID})
	return err
}
