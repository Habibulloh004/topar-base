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

type CartRepository struct {
	collection *mongo.Collection
}

func NewCartRepository(db *mongo.Database) *CartRepository {
	return &CartRepository{collection: db.Collection("carts")}
}

func (r *CartRepository) FindByUserID(ctx context.Context, userID primitive.ObjectID) (*models.Cart, error) {
	var cart models.Cart
	err := r.collection.FindOne(ctx, bson.M{"userId": userID}).Decode(&cart)
	if err != nil {
		return nil, err
	}
	return &cart, nil
}

func (r *CartRepository) Upsert(ctx context.Context, userID primitive.ObjectID) (*models.Cart, error) {
	now := time.Now()
	filter := bson.M{"userId": userID}
	update := bson.M{
		"$setOnInsert": bson.M{
			"_id":    primitive.NewObjectID(),
			"userId": userID,
			"items":  []models.CartItem{},
		},
		"$set": bson.M{"updatedAt": now},
	}
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var cart models.Cart
	err := r.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&cart)
	if err != nil {
		return nil, err
	}
	return &cart, nil
}

func (r *CartRepository) AddItem(ctx context.Context, userID primitive.ObjectID, item models.CartItem) error {
	// Push new item only when it is not already in the cart.
	res, err := r.collection.UpdateOne(ctx,
		bson.M{"userId": userID, "items.productId": bson.M{"$ne": item.ProductID}},
		bson.M{
			"$push": bson.M{"items": item},
			"$set":  bson.M{"updatedAt": time.Now()},
		},
	)
	if err != nil {
		return err
	}
	if res.MatchedCount == 0 {
		// Item already exists — increment its quantity.
		_, err = r.collection.UpdateOne(ctx,
			bson.M{"userId": userID, "items.productId": item.ProductID},
			bson.M{
				"$inc": bson.M{"items.$.quantity": item.Quantity},
				"$set": bson.M{"updatedAt": time.Now()},
			},
		)
		return err
	}
	return nil
}

func (r *CartRepository) UpdateItemQty(ctx context.Context, userID, productID primitive.ObjectID, quantity int) error {
	_, err := r.collection.UpdateOne(ctx,
		bson.M{"userId": userID, "items.productId": productID},
		bson.M{
			"$set": bson.M{
				"items.$.quantity": quantity,
				"updatedAt":        time.Now(),
			},
		},
	)
	return err
}

func (r *CartRepository) RemoveItem(ctx context.Context, userID, productID primitive.ObjectID) error {
	_, err := r.collection.UpdateOne(ctx,
		bson.M{"userId": userID},
		bson.M{
			"$pull": bson.M{"items": bson.M{"productId": productID}},
			"$set":  bson.M{"updatedAt": time.Now()},
		},
	)
	return err
}

func (r *CartRepository) Clear(ctx context.Context, userID primitive.ObjectID) error {
	_, err := r.collection.UpdateOne(ctx,
		bson.M{"userId": userID},
		bson.M{"$set": bson.M{"items": []models.CartItem{}, "updatedAt": time.Now()}},
	)
	return err
}

func (r *CartRepository) ReplaceItems(ctx context.Context, userID primitive.ObjectID, items []models.CartItem) error {
	_, err := r.collection.UpdateOne(ctx,
		bson.M{"userId": userID},
		bson.M{"$set": bson.M{"items": items, "updatedAt": time.Now()}},
	)
	return err
}
