package services

import (
	"context"
	"strings"
	"sync"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type CategoryLinker struct {
	categoryRepo *repository.CategoryRepository
	mu           sync.RWMutex

	// Cache of normalized category name -> ObjectID
	nameToID map[string]primitive.ObjectID

	// Cache of category ID -> full path names
	idToPath map[primitive.ObjectID][]string

	// Cache of category ID -> all ancestor IDs (including self)
	idToAncestors map[primitive.ObjectID][]primitive.ObjectID

	// Cache of category ID -> all descendant IDs (including self)
	idToDescendants map[primitive.ObjectID][]primitive.ObjectID

	// All category nodes for iteration
	allNodes []models.CategoryNode
}

func NewCategoryLinker(categoryRepo *repository.CategoryRepository) *CategoryLinker {
	return &CategoryLinker{
		categoryRepo:    categoryRepo,
		nameToID:        make(map[string]primitive.ObjectID),
		idToPath:        make(map[primitive.ObjectID][]string),
		idToAncestors:   make(map[primitive.ObjectID][]primitive.ObjectID),
		idToDescendants: make(map[primitive.ObjectID][]primitive.ObjectID),
	}
}

// BuildCache loads all categories and builds lookup maps
func (l *CategoryLinker) BuildCache(ctx context.Context) error {
	tree, err := l.categoryRepo.GetTree(ctx)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Reset caches
	l.nameToID = make(map[string]primitive.ObjectID)
	l.idToPath = make(map[primitive.ObjectID][]string)
	l.idToAncestors = make(map[primitive.ObjectID][]primitive.ObjectID)
	l.idToDescendants = make(map[primitive.ObjectID][]primitive.ObjectID)
	l.allNodes = nil

	// First pass: build nameToID, idToPath, idToAncestors
	l.walkTreeBuildAncestors(tree, nil, nil)

	// Second pass: build idToDescendants
	l.buildDescendants()

	return nil
}

func (l *CategoryLinker) walkTreeBuildAncestors(nodes []models.CategoryNode, path []string, ancestorIDs []primitive.ObjectID) {
	for _, node := range nodes {
		id, err := primitive.ObjectIDFromHex(node.ID)
		if err != nil {
			continue
		}

		currentPath := append([]string{}, path...)
		currentPath = append(currentPath, node.Name)

		currentAncestors := append([]primitive.ObjectID{}, ancestorIDs...)
		currentAncestors = append(currentAncestors, id)

		// Store normalized name (lowercase, trimmed)
		normalizedName := strings.ToLower(strings.TrimSpace(node.Name))
		l.nameToID[normalizedName] = id

		l.idToPath[id] = currentPath
		l.idToAncestors[id] = currentAncestors
		l.allNodes = append(l.allNodes, node)

		if len(node.Children) > 0 {
			l.walkTreeBuildAncestors(node.Children, currentPath, currentAncestors)
		}
	}
}

func (l *CategoryLinker) buildDescendants() {
	// For each category, find all categories that have it in their ancestors
	for id := range l.idToAncestors {
		descendants := []primitive.ObjectID{id}

		for otherId, ancestors := range l.idToAncestors {
			if otherId == id {
				continue
			}
			for _, ancestorID := range ancestors {
				if ancestorID == id {
					descendants = append(descendants, otherId)
					break
				}
			}
		}

		l.idToDescendants[id] = descendants
	}
}

// LinkProduct sets CategoryIDs and CategoryPath based on Subject/Niche
func (l *CategoryLinker) LinkProduct(product *models.EksmoProduct) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var categoryIDs []primitive.ObjectID
	var categoryPath []string

	// Try to match by Subject name
	if product.Subject != nil && product.Subject.Name != "" {
		normalizedSubject := strings.ToLower(strings.TrimSpace(product.Subject.Name))
		if id, ok := l.nameToID[normalizedSubject]; ok {
			categoryIDs = l.idToAncestors[id]
			categoryPath = l.idToPath[id]
		}
	}

	// Also try to match by Niche if Subject didn't match
	if len(categoryIDs) == 0 && product.Niche != nil && product.Niche.Name != "" {
		normalizedNiche := strings.ToLower(strings.TrimSpace(product.Niche.Name))
		if id, ok := l.nameToID[normalizedNiche]; ok {
			categoryIDs = l.idToAncestors[id]
			categoryPath = l.idToPath[id]
		}
	}

	// Try partial match if exact match failed
	if len(categoryIDs) == 0 {
		categoryIDs, categoryPath = l.findPartialMatch(product)
	}

	product.CategoryIDs = categoryIDs
	product.CategoryPath = categoryPath
}

// findPartialMatch tries to find a category that contains the subject/niche name
func (l *CategoryLinker) findPartialMatch(product *models.EksmoProduct) ([]primitive.ObjectID, []string) {
	searchTerms := []string{}

	if product.Subject != nil && product.Subject.Name != "" {
		searchTerms = append(searchTerms, strings.ToLower(strings.TrimSpace(product.Subject.Name)))
	}
	if product.Niche != nil && product.Niche.Name != "" {
		searchTerms = append(searchTerms, strings.ToLower(strings.TrimSpace(product.Niche.Name)))
	}

	for _, term := range searchTerms {
		for name, id := range l.nameToID {
			if strings.Contains(name, term) || strings.Contains(term, name) {
				return l.idToAncestors[id], l.idToPath[id]
			}
		}
	}

	return nil, nil
}

// GetCategoryAndDescendantIDs returns IDs for a category and all its descendants
// This is used for nested category filtering - selecting a parent category
// should also show products in all child categories
func (l *CategoryLinker) GetCategoryAndDescendantIDs(categoryID primitive.ObjectID) []primitive.ObjectID {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if descendants, ok := l.idToDescendants[categoryID]; ok {
		return descendants
	}

	return []primitive.ObjectID{categoryID}
}

// GetCategoryByID returns the category path for a given ID
func (l *CategoryLinker) GetCategoryPath(categoryID primitive.ObjectID) []string {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if path, ok := l.idToPath[categoryID]; ok {
		return path
	}
	return nil
}

// GetCategoryIDByName returns the category ID for a given name (case-insensitive)
func (l *CategoryLinker) GetCategoryIDByName(name string) (primitive.ObjectID, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	normalized := strings.ToLower(strings.TrimSpace(name))
	id, ok := l.nameToID[normalized]
	return id, ok
}

// IsCacheBuilt returns whether the cache has been built
func (l *CategoryLinker) IsCacheBuilt() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.nameToID) > 0
}

// GetStats returns statistics about the cached categories
func (l *CategoryLinker) GetStats() map[string]int {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return map[string]int{
		"totalCategories": len(l.nameToID),
		"totalNodes":      len(l.allNodes),
	}
}
