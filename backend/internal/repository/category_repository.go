package repository

import (
	"context"
	"fmt"

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

func (r *CategoryRepository) ListAll(ctx context.Context) ([]models.Category, error) {
	cursor, err := r.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var categories []models.Category
	if err := cursor.All(ctx, &categories); err != nil {
		return nil, err
	}
	return categories, nil
}

func (r *CategoryRepository) Create(ctx context.Context, name string, parentID string) (primitive.ObjectID, error) {
	category := models.Category{
		ID:       primitive.NewObjectID(),
		Name:     name,
		ParentID: parentID,
	}

	if _, err := r.collection.InsertOne(ctx, category); err != nil {
		return primitive.NilObjectID, err
	}
	return category.ID, nil
}

func (r *CategoryRepository) UpdateFields(ctx context.Context, id primitive.ObjectID, updates bson.M) error {
	if len(updates) == 0 {
		return nil
	}

	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, bson.M{"$set": updates})
	if err != nil {
		return err
	}
	if result.MatchedCount == 0 {
		return mongo.ErrNoDocuments
	}
	return nil
}

func (r *CategoryRepository) DeleteWithDescendants(ctx context.Context, rootID primitive.ObjectID) (int64, error) {
	categories, err := r.ListAll(ctx)
	if err != nil {
		return 0, err
	}

	rootHex := rootID.Hex()
	childrenByParent := make(map[string][]string, len(categories))
	for _, category := range categories {
		childrenByParent[category.ParentID] = append(childrenByParent[category.ParentID], category.ID.Hex())
	}

	toDelete := make([]primitive.ObjectID, 0, 8)
	seen := make(map[string]struct{}, 8)
	stack := []string{rootHex}

	for len(stack) > 0 {
		last := len(stack) - 1
		current := stack[last]
		stack = stack[:last]
		if _, exists := seen[current]; exists {
			continue
		}
		seen[current] = struct{}{}

		objectID, parseErr := primitive.ObjectIDFromHex(current)
		if parseErr != nil {
			return 0, fmt.Errorf("invalid category id %q: %w", current, parseErr)
		}
		toDelete = append(toDelete, objectID)

		for _, childID := range childrenByParent[current] {
			if _, visited := seen[childID]; visited {
				continue
			}
			stack = append(stack, childID)
		}
	}

	if len(toDelete) == 0 {
		return 0, nil
	}

	result, err := r.collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": toDelete}})
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}
