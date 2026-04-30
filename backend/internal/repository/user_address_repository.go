package repository

import (
	"context"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type UserAddressRepository struct {
	collection *mongo.Collection
}

func NewUserAddressRepository(db *mongo.Database) *UserAddressRepository {
	return &UserAddressRepository{collection: db.Collection("user_addresses")}
}

func (r *UserAddressRepository) FindByUserID(ctx context.Context, userID primitive.ObjectID) ([]models.UserAddress, error) {
	cursor, err := r.collection.Find(ctx, bson.M{"userId": userID})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var addresses []models.UserAddress
	if err := cursor.All(ctx, &addresses); err != nil {
		return nil, err
	}
	return addresses, nil
}

func (r *UserAddressRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*models.UserAddress, error) {
	var addr models.UserAddress
	err := r.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&addr)
	if err != nil {
		return nil, err
	}
	return &addr, nil
}

func (r *UserAddressRepository) Create(ctx context.Context, addr *models.UserAddress) error {
	addr.ID = primitive.NewObjectID()
	addr.CreatedAt = time.Now()
	addr.UpdatedAt = time.Now()
	_, err := r.collection.InsertOne(ctx, addr)
	return err
}

func (r *UserAddressRepository) Update(ctx context.Context, id primitive.ObjectID, fields bson.M) error {
	fields["updatedAt"] = time.Now()
	_, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": fields})
	return err
}

func (r *UserAddressRepository) Delete(ctx context.Context, id primitive.ObjectID) error {
	_, err := r.collection.DeleteOne(ctx, bson.M{"_id": id})
	return err
}
