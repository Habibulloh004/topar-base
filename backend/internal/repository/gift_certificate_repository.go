package repository

import (
	"context"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type GiftCertificateRepository struct {
	collection *mongo.Collection
}

func NewGiftCertificateRepository(db *mongo.Database) *GiftCertificateRepository {
	return &GiftCertificateRepository{collection: db.Collection("gift_certificates")}
}

func (r *GiftCertificateRepository) ListActive(ctx context.Context) ([]models.GiftCertificate, error) {
	opts := options.Find().SetSort(bson.D{{Key: "sortOrder", Value: 1}, {Key: "amount", Value: 1}})
	cursor, err := r.collection.Find(ctx, bson.M{"isActive": true}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var certs []models.GiftCertificate
	if err := cursor.All(ctx, &certs); err != nil {
		return nil, err
	}
	return certs, nil
}

func (r *GiftCertificateRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*models.GiftCertificate, error) {
	var cert models.GiftCertificate
	err := r.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&cert)
	if err != nil {
		return nil, err
	}
	return &cert, nil
}
