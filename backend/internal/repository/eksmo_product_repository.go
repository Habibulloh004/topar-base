package repository

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type EksmoProductRepository struct {
	collection *mongo.Collection
}

func NewEksmoProductRepository(db *mongo.Database) *EksmoProductRepository {
	return &EksmoProductRepository{collection: db.Collection("eksmo_products")}
}

func (r *EksmoProductRepository) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		// Primary unique identifier
		{
			Keys:    bson.D{{Key: "guidNom", Value: 1}},
			Options: options.Index().SetName("guidNom_unique").SetUnique(true).SetSparse(true),
		},
		// Existing indexes
		{
			Keys:    bson.D{{Key: "guid", Value: 1}},
			Options: options.Index().SetName("guid_idx"),
		},
		{
			Keys:    bson.D{{Key: "name", Value: 1}},
			Options: options.Index().SetName("name_idx"),
		},
		{
			Keys:    bson.D{{Key: "subjectName", Value: 1}},
			Options: options.Index().SetName("subject_idx"),
		},
		{
			Keys:    bson.D{{Key: "updatedAt", Value: -1}},
			Options: options.Index().SetName("updatedAt_desc_idx"),
		},
		// New indexes for filtering
		{
			Keys:    bson.D{{Key: "categoryIds", Value: 1}},
			Options: options.Index().SetName("categoryIds_idx"),
		},
		{
			Keys:    bson.D{{Key: "authorRefs.guid", Value: 1}},
			Options: options.Index().SetName("authorGuid_idx"),
		},
		{
			Keys:    bson.D{{Key: "authorNames", Value: 1}},
			Options: options.Index().SetName("authorNames_idx"),
		},
		{
			Keys:    bson.D{{Key: "tagRefs.guid", Value: 1}},
			Options: options.Index().SetName("tagGuid_idx"),
		},
		{
			Keys:    bson.D{{Key: "tagNames", Value: 1}},
			Options: options.Index().SetName("tagNames_idx"),
		},
		{
			Keys:    bson.D{{Key: "genreNames", Value: 1}},
			Options: options.Index().SetName("genreNames_idx"),
		},
		{
			Keys:    bson.D{{Key: "series.guid", Value: 1}},
			Options: options.Index().SetName("seriesGuid_idx"),
		},
		{
			Keys:    bson.D{{Key: "publisherRef.guid", Value: 1}},
			Options: options.Index().SetName("publisherGuid_idx"),
		},
		{
			Keys:    bson.D{{Key: "ageRestriction", Value: 1}},
			Options: options.Index().SetName("ageRestriction_idx"),
		},
		{
			Keys:    bson.D{{Key: "brandName", Value: 1}},
			Options: options.Index().SetName("brandName_idx"),
		},
		{
			Keys:    bson.D{{Key: "serieName", Value: 1}},
			Options: options.Index().SetName("serieName_idx"),
		},
		{
			Keys:    bson.D{{Key: "publisher", Value: 1}},
			Options: options.Index().SetName("publisher_idx"),
		},
		{
			Keys:    bson.D{{Key: "subject.guid", Value: 1}},
			Options: options.Index().SetName("subjectGuid_idx"),
		},
		{
			Keys:    bson.D{{Key: "niche.guid", Value: 1}},
			Options: options.Index().SetName("nicheGuid_idx"),
		},
	}

	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *EksmoProductRepository) UpsertBatch(ctx context.Context, products []models.EksmoProduct) (upserted int, modified int, skipped int, err error) {
	if len(products) == 0 {
		return 0, 0, 0, nil
	}

	operations := make([]mongo.WriteModel, 0, len(products))
	seenKeys := make(map[string]struct{}, len(products))
	now := time.Now().UTC()
	for _, p := range products {
		filter, ferr := buildEksmoFilter(p)
		if ferr != nil {
			skipped++
			continue
		}
		stableKey := stableFilterKey(filter)
		if _, exists := seenKeys[stableKey]; exists {
			skipped++
			continue
		}
		seenKeys[stableKey] = struct{}{}

		p.UpdatedAt = now
		update := bson.M{"$set": p}
		operations = append(operations, mongo.NewUpdateOneModel().SetFilter(filter).SetUpdate(update).SetUpsert(true))
	}

	if len(operations) == 0 {
		return 0, 0, skipped, nil
	}

	result, err := r.collection.BulkWrite(ctx, operations, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, 0, skipped, err
	}

	return int(result.UpsertedCount), int(result.ModifiedCount), skipped, nil
}

func buildEksmoFilter(product models.EksmoProduct) (bson.M, error) {
	if product.GUIDNOM != "" {
		return bson.M{"guidNom": product.GUIDNOM}, nil
	}
	if product.GUID != "" {
		return bson.M{"guid": product.GUID}, nil
	}
	if product.NomCode != "" {
		return bson.M{"nomcode": product.NomCode}, nil
	}
	return nil, errors.New("missing stable identifier")
}

func stableFilterKey(filter bson.M) string {
	if value, ok := filter["guidNom"].(string); ok {
		return "guidNom:" + value
	}
	if value, ok := filter["guid"].(string); ok {
		return "guid:" + value
	}
	if value, ok := filter["nomcode"].(string); ok {
		return "nomcode:" + value
	}
	return ""
}

// ProductFilterParams contains all possible filter options
type ProductFilterParams struct {
	Page            int64
	Limit           int64
	Search          string
	CategoryID      primitive.ObjectID
	CategoryIDs     []primitive.ObjectID // For nested category filtering
	AuthorGUIDs     []string
	AuthorName      string
	AuthorNames     []string
	TagGUIDs        []string
	TagName         string
	GenreNames      []string
	SeriesGUID      string
	SeriesName      string
	SeriesNames     []string
	PublisherGUID   string
	PublisherName   string
	PublisherNames  []string
	Subject         string
	Brand           string
	Brands          []string
	AgeRestriction  string
	AgeRestrictions []string
	SubjectGUIDs    []string // Filter by subject GUIDs (multiple allowed)
	NicheGUIDs      []string // Filter by niche GUIDs (includes all subjects under these niches)
}

type EksmoDuplicateScanRecord struct {
	ID      primitive.ObjectID `bson:"_id"`
	ISBN    string             `bson:"isbn"`
	NomCode string             `bson:"nomcode"`
}

// ListWithFilters returns products matching the given filter parameters
func (r *EksmoProductRepository) ListWithFilters(ctx context.Context, params ProductFilterParams) ([]models.EksmoProduct, int64, error) {
	if params.Page < 1 {
		params.Page = 1
	}
	if params.Limit < 1 {
		params.Limit = 20
	}
	if params.Limit > 200 {
		params.Limit = 200
	}

	filter := bson.M{}
	andClauses := []bson.M{}

	// Text search on multiple fields
	if params.Search != "" {
		andClauses = append(andClauses, bson.M{"$or": []bson.M{
			{"name": bson.M{"$regex": params.Search, "$options": "i"}},
			{"authorCover": bson.M{"$regex": params.Search, "$options": "i"}},
			{"isbn": bson.M{"$regex": params.Search, "$options": "i"}},
			{"authorNames": bson.M{"$regex": params.Search, "$options": "i"}},
		}})
	}

	// Category filter (includes nested)
	if len(params.CategoryIDs) > 0 {
		filter["categoryIds"] = bson.M{"$in": params.CategoryIDs}
	} else if !params.CategoryID.IsZero() {
		filter["categoryIds"] = params.CategoryID
	}

	// Author filters
	if len(params.AuthorGUIDs) > 0 {
		filter["authorRefs.guid"] = bson.M{"$in": params.AuthorGUIDs}
	}
	if len(params.AuthorNames) > 0 {
		authorNameClauses := make([]bson.M, 0, len(params.AuthorNames))
		for _, authorName := range params.AuthorNames {
			trimmed := strings.TrimSpace(authorName)
			if trimmed == "" {
				continue
			}
			authorNameClauses = append(authorNameClauses, bson.M{"authorNames": bson.M{"$regex": regexp.QuoteMeta(trimmed), "$options": "i"}})
		}
		if len(authorNameClauses) > 0 {
			andClauses = append(andClauses, bson.M{"$or": authorNameClauses})
		}
	} else if params.AuthorName != "" {
		filter["authorNames"] = bson.M{"$regex": regexp.QuoteMeta(params.AuthorName), "$options": "i"}
	}

	// Tag filters
	if len(params.TagGUIDs) > 0 {
		filter["tagRefs.guid"] = bson.M{"$in": params.TagGUIDs}
	}
	if params.TagName != "" {
		filter["tagNames"] = bson.M{"$regex": regexp.QuoteMeta(params.TagName), "$options": "i"}
	}

	// Genre filter
	if len(params.GenreNames) > 0 {
		filter["genreNames"] = bson.M{"$in": params.GenreNames}
	}

	// Series filter
	if params.SeriesGUID != "" {
		filter["series.guid"] = params.SeriesGUID
	}
	if len(params.SeriesNames) > 0 {
		filter["serieName"] = bson.M{"$in": params.SeriesNames}
	} else if params.SeriesName != "" {
		filter["serieName"] = bson.M{"$regex": regexp.QuoteMeta(params.SeriesName), "$options": "i"}
	}

	// Publisher filter
	if params.PublisherGUID != "" {
		filter["publisherRef.guid"] = params.PublisherGUID
	}
	if len(params.PublisherNames) > 0 {
		filter["publisher"] = bson.M{"$in": params.PublisherNames}
	} else if params.PublisherName != "" {
		filter["publisher"] = bson.M{"$regex": regexp.QuoteMeta(params.PublisherName), "$options": "i"}
	}

	// Legacy filters for backward compatibility
	if params.Subject != "" {
		filter["subjectName"] = bson.M{"$regex": regexp.QuoteMeta(params.Subject), "$options": "i"}
	}
	if len(params.Brands) > 0 {
		filter["brandName"] = bson.M{"$in": params.Brands}
	} else if params.Brand != "" {
		filter["brandName"] = bson.M{"$regex": regexp.QuoteMeta(params.Brand), "$options": "i"}
	}

	// Age restriction
	if len(params.AgeRestrictions) > 0 {
		filter["ageRestriction"] = bson.M{"$in": params.AgeRestrictions}
	} else if params.AgeRestriction != "" {
		filter["ageRestriction"] = params.AgeRestriction
	}

	// Subject and Niche GUIDs filter - use $or if both are provided
	hasSubjects := len(params.SubjectGUIDs) > 0
	hasNiches := len(params.NicheGUIDs) > 0

	if hasSubjects && hasNiches {
		// Both subjects and niches selected - match products in either
		andClauses = append(andClauses, bson.M{"$or": []bson.M{
			{"subject.guid": bson.M{"$in": params.SubjectGUIDs}},
			{"niche.guid": bson.M{"$in": params.NicheGUIDs}},
		}})
	} else if hasSubjects {
		filter["subject.guid"] = bson.M{"$in": params.SubjectGUIDs}
	} else if hasNiches {
		filter["niche.guid"] = bson.M{"$in": params.NicheGUIDs}
	}

	finalFilter := filter
	if len(andClauses) > 0 {
		if len(filter) > 0 {
			andClauses = append([]bson.M{filter}, andClauses...)
		}
		finalFilter = bson.M{"$and": andClauses}
	}

	total, err := r.collection.CountDocuments(ctx, finalFilter)
	if err != nil {
		return nil, 0, err
	}

	opts := options.Find().
		SetSkip((params.Page - 1) * params.Limit).
		SetLimit(params.Limit).
		SetSort(bson.D{{Key: "updatedAt", Value: -1}})

	cursor, err := r.collection.Find(ctx, finalFilter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	var products []models.EksmoProduct
	if err := cursor.All(ctx, &products); err != nil {
		return nil, 0, err
	}

	return products, total, nil
}

// ListByIDs returns products by explicit MongoDB object IDs.
func (r *EksmoProductRepository) ListByIDs(ctx context.Context, ids []primitive.ObjectID) ([]models.EksmoProduct, error) {
	if len(ids) == 0 {
		return []models.EksmoProduct{}, nil
	}

	cursor, err := r.collection.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var products []models.EksmoProduct
	if err := cursor.All(ctx, &products); err != nil {
		return nil, err
	}

	return products, nil
}

// ListByIDsLite returns products by explicit MongoDB object IDs without the heavy raw payload.
func (r *EksmoProductRepository) ListByIDsLite(ctx context.Context, ids []primitive.ObjectID) ([]models.EksmoProduct, error) {
	if len(ids) == 0 {
		return []models.EksmoProduct{}, nil
	}

	opts := options.Find().SetProjection(bson.M{
		"raw": 0,
	})
	cursor, err := r.collection.Find(ctx, bson.M{"_id": bson.M{"$in": ids}}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var products []models.EksmoProduct
	if err := cursor.All(ctx, &products); err != nil {
		return nil, err
	}

	return products, nil
}

// ListDuplicateScanRecords returns lightweight records required for duplicate detection.
func (r *EksmoProductRepository) ListDuplicateScanRecords(ctx context.Context) ([]EksmoDuplicateScanRecord, error) {
	opts := options.Find().SetProjection(bson.M{
		"_id":     1,
		"isbn":    1,
		"nomcode": 1,
	})
	cursor, err := r.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var rows []EksmoDuplicateScanRecord
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// List is the legacy method for backward compatibility
func (r *EksmoProductRepository) List(ctx context.Context, page, limit int64, search, subject, brand, serie, publisher string) ([]models.EksmoProduct, int64, error) {
	return r.ListWithFilters(ctx, ProductFilterParams{
		Page:          page,
		Limit:         limit,
		Search:        search,
		Subject:       subject,
		Brand:         brand,
		SeriesName:    serie,
		PublisherName: publisher,
	})
}

// Meta returns distinct values for filter dropdowns (legacy)
func (r *EksmoProductRepository) Meta(ctx context.Context, limit int) (models.EksmoMeta, error) {
	subjects, err := r.distinctStrings(ctx, "subjectName", limit)
	if err != nil {
		return models.EksmoMeta{}, err
	}
	brands, err := r.distinctStrings(ctx, "brandName", limit)
	if err != nil {
		return models.EksmoMeta{}, err
	}
	series, err := r.distinctStrings(ctx, "serieName", limit)
	if err != nil {
		return models.EksmoMeta{}, err
	}
	publishers, err := r.distinctStrings(ctx, "publisher", limit)
	if err != nil {
		return models.EksmoMeta{}, err
	}

	return models.EksmoMeta{
		Subjects:   subjects,
		Brands:     brands,
		Series:     series,
		Publishers: publishers,
	}, nil
}

// MetaExpanded returns expanded metadata including authors, tags, genres
func (r *EksmoProductRepository) MetaExpanded(ctx context.Context, limit int) (models.EksmoMeta, error) {
	meta := models.EksmoMeta{}

	// Existing distinct queries
	meta.Subjects, _ = r.distinctStrings(ctx, "subjectName", limit)
	meta.Brands, _ = r.distinctStrings(ctx, "brandName", limit)
	meta.Series, _ = r.distinctStrings(ctx, "serieName", limit)
	meta.Publishers, _ = r.distinctStrings(ctx, "publisher", limit)

	// New distinct queries for arrays
	meta.Tags, _ = r.distinctStringsFromArray(ctx, "tagNames", limit)
	meta.Genres, _ = r.distinctStringsFromArray(ctx, "genreNames", limit)

	// Authors with GUID for filtering
	meta.Authors, _ = r.getUniqueAuthors(ctx, limit)

	return meta, nil
}

func (r *EksmoProductRepository) distinctStrings(ctx context.Context, field string, limit int) ([]string, error) {
	values, err := r.collection.Distinct(ctx, field, bson.M{
		field: bson.M{
			"$type": "string",
			"$ne":   "",
		},
	})
	if err != nil {
		return nil, err
	}

	items := make([]string, 0, len(values))
	for _, value := range values {
		typed, ok := value.(string)
		if !ok {
			continue
		}
		typed = strings.TrimSpace(typed)
		if typed == "" {
			continue
		}
		items = append(items, typed)
	}

	sort.Strings(items)
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (r *EksmoProductRepository) distinctStringsFromArray(ctx context.Context, field string, limit int) ([]string, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{field: bson.M{"$exists": true, "$ne": nil}}}},
		{{Key: "$unwind", Value: "$" + field}},
		{{Key: "$group", Value: bson.M{"_id": "$" + field}}},
		{{Key: "$match", Value: bson.M{"_id": bson.M{"$ne": ""}}}},
		{{Key: "$sort", Value: bson.M{"_id": 1}}},
	}
	if limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: limit}})
	}

	cursor, err := r.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var results []string
	for cursor.Next(ctx) {
		var result struct {
			ID string `bson:"_id"`
		}
		if cursor.Decode(&result) == nil && result.ID != "" {
			results = append(results, result.ID)
		}
	}

	return results, nil
}

func (r *EksmoProductRepository) getUniqueAuthors(ctx context.Context, limit int) ([]models.EksmoAuthorMeta, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"authorRefs": bson.M{"$exists": true, "$ne": nil}}}},
		{{Key: "$unwind", Value: "$authorRefs"}},
		{{Key: "$match", Value: bson.M{"authorRefs.guid": bson.M{"$ne": ""}}}},
		{{Key: "$group", Value: bson.M{
			"_id":  "$authorRefs.guid",
			"name": bson.M{"$first": "$authorRefs.name"},
		}}},
		{{Key: "$match", Value: bson.M{"name": bson.M{"$ne": ""}}}},
		{{Key: "$sort", Value: bson.M{"name": 1}}},
	}
	if limit > 0 {
		pipeline = append(pipeline, bson.D{{Key: "$limit", Value: limit}})
	}

	cursor, err := r.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var authors []models.EksmoAuthorMeta
	for cursor.Next(ctx) {
		var result struct {
			ID   string `bson:"_id"`
			Name string `bson:"name"`
		}
		if cursor.Decode(&result) == nil && result.ID != "" && result.Name != "" {
			authors = append(authors, models.EksmoAuthorMeta{
				GUID: result.ID,
				Name: result.Name,
			})
		}
	}

	return authors, nil
}

// LinkProductWithCategory sets categoryIds for a single product
func (r *EksmoProductRepository) LinkProductWithCategory(ctx context.Context, productID primitive.ObjectID, categoryIDs []primitive.ObjectID, categoryPath []string) error {
	_, err := r.collection.UpdateOne(ctx,
		bson.M{"_id": productID},
		bson.M{"$set": bson.M{
			"categoryIds":  categoryIDs,
			"categoryPath": categoryPath,
			"updatedAt":    time.Now().UTC(),
		}},
	)
	return err
}

// LinkAllProductsWithCategories iterates all products and links them using the provided linker function
func (r *EksmoProductRepository) LinkAllProductsWithCategories(ctx context.Context, linkFunc func(*models.EksmoProduct)) (int, error) {
	cursor, err := r.collection.Find(ctx, bson.M{})
	if err != nil {
		return 0, err
	}
	defer cursor.Close(ctx)

	linked := 0
	var operations []mongo.WriteModel

	for cursor.Next(ctx) {
		var product models.EksmoProduct
		if err := cursor.Decode(&product); err != nil {
			continue
		}

		linkFunc(&product)

		if len(product.CategoryIDs) > 0 {
			operations = append(operations,
				mongo.NewUpdateOneModel().
					SetFilter(bson.M{"_id": product.ID}).
					SetUpdate(bson.M{"$set": bson.M{
						"categoryIds":  product.CategoryIDs,
						"categoryPath": product.CategoryPath,
						"updatedAt":    time.Now().UTC(),
					}}),
			)
			linked++
		}

		// Batch write every 500 operations
		if len(operations) >= 500 {
			_, err := r.collection.BulkWrite(ctx, operations, options.BulkWrite().SetOrdered(false))
			if err != nil {
				return linked, err
			}
			operations = operations[:0]
		}
	}

	// Write remaining operations
	if len(operations) > 0 {
		_, err := r.collection.BulkWrite(ctx, operations, options.BulkWrite().SetOrdered(false))
		if err != nil {
			return linked, err
		}
	}

	return linked, nil
}

// Count returns total number of products
func (r *EksmoProductRepository) Count(ctx context.Context) (int64, error) {
	return r.collection.CountDocuments(ctx, bson.M{})
}

// GetByID returns a product by its ObjectID
func (r *EksmoProductRepository) GetByID(ctx context.Context, id primitive.ObjectID) (*models.EksmoProduct, error) {
	var product models.EksmoProduct
	err := r.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&product)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &product, nil
}

// GetByGUID returns a product by its GUID
func (r *EksmoProductRepository) GetByGUID(ctx context.Context, guid string) (*models.EksmoProduct, error) {
	var product models.EksmoProduct
	err := r.collection.FindOne(ctx, bson.M{"guid": guid}).Decode(&product)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &product, nil
}

// DeleteByID removes a single Eksmo product by Mongo ObjectID.
func (r *EksmoProductRepository) DeleteByID(ctx context.Context, id primitive.ObjectID) (bool, error) {
	result, err := r.collection.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return false, err
	}
	return result.DeletedCount > 0, nil
}

// DeleteByIDs removes Eksmo products by explicit Mongo ObjectIDs and returns deleted count.
func (r *EksmoProductRepository) DeleteByIDs(ctx context.Context, ids []primitive.ObjectID) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	result, err := r.collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return 0, err
	}
	return result.DeletedCount, nil
}

// SetInMainProductsByIDs sets the inMainProducts flag for explicit product IDs.
func (r *EksmoProductRepository) SetInMainProductsByIDs(ctx context.Context, ids []primitive.ObjectID, inMainProducts bool) error {
	if len(ids) == 0 {
		return nil
	}

	update := bson.M{
		"$set": bson.M{
			"inMainProducts": inMainProducts,
			"updatedAt":      time.Now().UTC(),
		},
	}

	_, err := r.collection.UpdateMany(ctx, bson.M{"_id": bson.M{"$in": ids}}, update)
	return err
}

// SetInMainProductsBySource sets the inMainProducts flag for products matched by stable source key.
func (r *EksmoProductRepository) SetInMainProductsBySource(ctx context.Context, guidNom, guid, nomcode string, inMainProducts bool) error {
	filter := bson.M{}
	switch {
	case guidNom != "":
		filter["guidNom"] = guidNom
	case guid != "":
		filter["guid"] = guid
	case nomcode != "":
		filter["nomcode"] = nomcode
	default:
		return nil
	}

	update := bson.M{
		"$set": bson.M{
			"inMainProducts": inMainProducts,
			"updatedAt":      time.Now().UTC(),
		},
	}

	_, err := r.collection.UpdateMany(ctx, filter, update)
	return err
}
