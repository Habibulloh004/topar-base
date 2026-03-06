package repository

import (
	"context"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type CategoryRepository struct {
	collection *mongo.Collection
}

func NewCategoryRepository(db *mongo.Database) *CategoryRepository {
	return &CategoryRepository{collection: db.Collection("main_categories")}
}

func (r *CategoryRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}

func (r *CategoryRepository) ReplaceFromInputs(ctx context.Context, items []models.CategoryInput) (int, error) {
	if _, err := r.collection.DeleteMany(ctx, bson.M{}); err != nil {
		return 0, err
	}

	inserted := 0
	for _, item := range items {
		count, err := r.insertNodeRecursive(ctx, item, "0")
		if err != nil {
			return inserted, err
		}
		inserted += count
	}

	return inserted, nil
}

func (r *CategoryRepository) insertNodeRecursive(ctx context.Context, item models.CategoryInput, parentID string) (int, error) {
	category := models.Category{
		ID:       primitive.NewObjectID(),
		Name:     item.Name,
		ParentID: parentID,
	}

	if _, err := r.collection.InsertOne(ctx, category); err != nil {
		return 0, err
	}

	inserted := 1
	for _, child := range item.Children {
		count, err := r.insertNodeRecursive(ctx, child, category.ID.Hex())
		if err != nil {
			return inserted, err
		}
		inserted += count
	}

	return inserted, nil
}

func (r *CategoryRepository) GetTree(ctx context.Context) ([]models.CategoryNode, error) {
	cursor, err := r.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var categories []models.Category
	if err := cursor.All(ctx, &categories); err != nil {
		return nil, err
	}

	nodes := make(map[string]models.CategoryNode, len(categories))
	childrenMap := make(map[string][]string, len(categories))

	for _, category := range categories {
		id := category.ID.Hex()
		nodes[id] = models.CategoryNode{
			ID:       id,
			Name:     category.Name,
			ParentID: category.ParentID,
		}
		childrenMap[category.ParentID] = append(childrenMap[category.ParentID], id)
	}

	var build func(parentID string) []models.CategoryNode
	build = func(parentID string) []models.CategoryNode {
		ids := childrenMap[parentID]
		result := make([]models.CategoryNode, 0, len(ids))
		for _, id := range ids {
			node := nodes[id]
			node.Children = build(node.ID)
			result = append(result, node)
		}
		return result
	}

	return build("0"), nil
}
