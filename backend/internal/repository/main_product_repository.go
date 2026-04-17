package repository

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MainProductRepository struct {
	collection *mongo.Collection
}

func NewMainProductRepository(db *mongo.Database) *MainProductRepository {
	return &MainProductRepository{collection: db.Collection("main_products")}
}

func (r *MainProductRepository) EnsureIndexes(ctx context.Context) error {
	indexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "sourceGuidNom", Value: 1}},
			Options: options.Index().SetName("sourceGuidNom_unique").SetUnique(true).SetSparse(true),
		},
		{
			Keys:    bson.D{{Key: "sourceProductId", Value: 1}},
			Options: options.Index().SetName("sourceProductId_unique").SetUnique(true).SetSparse(true),
		},
		{
			Keys:    bson.D{{Key: "sourceGuid", Value: 1}},
			Options: options.Index().SetName("sourceGuid_idx"),
		},
		{
			Keys:    bson.D{{Key: "sourceNomcode", Value: 1}},
			Options: options.Index().SetName("sourceNomcode_idx"),
		},
		{
			Keys:    bson.D{{Key: "categoryId", Value: 1}},
			Options: options.Index().SetName("categoryId_idx"),
		},
		{
			Keys:    bson.D{{Key: "isbnNormalized", Value: 1}},
			Options: options.Index().SetName("isbnNormalized_idx"),
		},
		{
			Keys:    bson.D{{Key: "updatedAt", Value: -1}},
			Options: options.Index().SetName("updatedAt_desc_idx"),
		},
	}

	_, err := r.collection.Indexes().CreateMany(ctx, indexes)
	return err
}

func (r *MainProductRepository) UpsertFromEksmoProducts(
	ctx context.Context,
	products []models.EksmoProduct,
	categoryID primitive.ObjectID,
	categoryPath []string,
) (upserted int, modified int, skipped int, err error) {
	if len(products) == 0 {
		return 0, 0, 0, nil
	}

	now := time.Now().UTC()
	operations := make([]mongo.WriteModel, 0, len(products))
	seen := make(map[string]struct{}, len(products))
	fallbackCategoryPath := sanitizeStringSlice(categoryPath)

	for _, p := range products {
		filter, ferr := buildMainProductSourceFilter(p)
		if ferr != nil {
			skipped++
			continue
		}

		key := stableMainSourceFilterKey(filter)
		if key == "" {
			skipped++
			continue
		}
		if _, exists := seen[key]; exists {
			skipped++
			continue
		}
		seen[key] = struct{}{}

		resolvedCategoryID := categoryID
		resolvedCategoryPath := fallbackCategoryPath
		if resolvedCategoryID.IsZero() {
			resolvedCategoryID, resolvedCategoryPath = defaultMainCategoryFromEksmoProduct(p)
		}

		doc := models.MainProduct{
			SourceProductID: p.ID,
			SourceGUID:      p.GUID,
			SourceGUIDNOM:   p.GUIDNOM,
			SourceNomCode:   p.NomCode,
			ISBN:            p.ISBN,
			ISBNNormalized:  normalizeBillzISBN(p.ISBN),
			Name:            p.Name,
			AuthorCover:     p.AuthorCover,
			AuthorNames:     p.AuthorNames,
			AuthorRefs:      p.AuthorRefs,
			TagRefs:         p.TagRefs,
			GenreRefs:       p.GenreRefs,
			TagNames:        p.TagNames,
			GenreNames:      p.GenreNames,
			Description:     p.Annotation,
			Annotation:      p.Annotation,
			CoverURL:        p.CoverURL,
			Pages:           p.Pages,
			Format:          p.Format,
			PaperType:       p.PaperType,
			BindingType:     p.BindingType,
			AgeRestriction:  p.AgeRestriction,
			SubjectName:     productSubjectName(p),
			NicheName:       productNicheName(p),
			BrandName:       productBrandName(p),
			SeriesName:      productSeriesName(p),
			PublisherName:   productPublisherName(p),
			CategoryID:      resolvedCategoryID,
			CategoryPath:    append([]string{}, resolvedCategoryPath...),
			UpdatedAt:       now,
		}
		if price, ok := extractEksmoPrice(p.Raw); ok {
			doc.Price = price
		}

		update := bson.M{
			"$set": doc,
			"$setOnInsert": bson.M{
				"createdAt": now,
			},
		}

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

func buildMainProductSourceFilter(product models.EksmoProduct) (bson.M, error) {
	if product.GUIDNOM != "" {
		return bson.M{"sourceGuidNom": product.GUIDNOM}, nil
	}
	if product.GUID != "" {
		return bson.M{"sourceGuid": product.GUID}, nil
	}
	if product.NomCode != "" {
		return bson.M{"sourceNomcode": product.NomCode}, nil
	}
	if !product.ID.IsZero() {
		return bson.M{"sourceProductId": product.ID}, nil
	}
	return nil, errors.New("missing stable source identifier")
}

func stableMainSourceFilterKey(filter bson.M) string {
	if value, ok := filter["sourceGuidNom"].(string); ok && value != "" {
		return "sourceGuidNom:" + value
	}
	if value, ok := filter["sourceGuid"].(string); ok && value != "" {
		return "sourceGuid:" + value
	}
	if value, ok := filter["sourceNomcode"].(string); ok && value != "" {
		return "sourceNomcode:" + value
	}
	if value, ok := filter["sourceProductId"].(primitive.ObjectID); ok && !value.IsZero() {
		return "sourceProductId:" + value.Hex()
	}
	return ""
}

func productSubjectName(product models.EksmoProduct) string {
	if product.Subject != nil && product.Subject.Name != "" {
		return product.Subject.Name
	}
	return product.SubjectName
}

func productNicheName(product models.EksmoProduct) string {
	if product.Niche != nil {
		return product.Niche.Name
	}
	return ""
}

func productBrandName(product models.EksmoProduct) string {
	if product.Brand != nil && product.Brand.Name != "" {
		return product.Brand.Name
	}
	return product.BrandName
}

func productSeriesName(product models.EksmoProduct) string {
	if product.Series != nil && product.Series.Name != "" {
		return product.Series.Name
	}
	return product.SerieName
}

func productPublisherName(product models.EksmoProduct) string {
	if product.Publisher != nil && product.Publisher.Name != "" {
		return product.Publisher.Name
	}
	return product.PublisherName
}

func defaultMainCategoryFromEksmoProduct(product models.EksmoProduct) (primitive.ObjectID, []string) {
	var categoryID primitive.ObjectID
	for index := len(product.CategoryIDs) - 1; index >= 0; index-- {
		if product.CategoryIDs[index].IsZero() {
			continue
		}
		categoryID = product.CategoryIDs[index]
		break
	}
	return categoryID, sanitizeStringSlice(product.CategoryPath)
}

type MainProductFilterParams struct {
	Page                int64
	Limit               int64
	Search              string
	CategoryID          primitive.ObjectID
	CategoryIDs         []primitive.ObjectID
	ExcludeIDs          []primitive.ObjectID
	WithoutCategory     bool
	WithoutISBN         bool
	BillzSyncable       *bool
	InfoComplete        *bool
	IncludeEksmoSources bool
	SourceDomains       []string
	SourceCategoryPaths [][]string
	OtherCategoryPaths  [][]string
}

type MainProductBillzSyncCandidate struct {
	ID             primitive.ObjectID `bson:"_id"`
	ISBN           string             `bson:"isbn"`
	ISBNNormalized string             `bson:"isbnNormalized"`
	Quantity       *float64           `bson:"quantity,omitempty"`
	Price          *float64           `bson:"price,omitempty"`
}

type MainProductBillzSyncUpdate struct {
	ID             primitive.ObjectID
	ISBNNormalized string
	Quantity       float64
	Price          float64
}

type MainProductDuplicateScanRecord struct {
	ID             primitive.ObjectID `bson:"_id"`
	ISBN           string             `bson:"isbn"`
	ISBNNormalized string             `bson:"isbnNormalized"`
	Barcode        string             `bson:"barcode"`
	Code           string             `bson:"code"`
	SourceNomCode  string             `bson:"sourceNomcode"`
}

func (r *MainProductRepository) ListWithFilters(ctx context.Context, params MainProductFilterParams) ([]models.MainProduct, int64, error) {
	if params.Page < 1 {
		params.Page = 1
	}
	if params.Limit < 1 {
		params.Limit = 20
	}
	if params.Limit > 200 {
		params.Limit = 200
	}

	filter := buildMainProductFilter(params)

	total, err := r.collection.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	if strings.TrimSpace(params.Search) == "" {
		opts := options.Find().
			SetSkip((params.Page - 1) * params.Limit).
			SetLimit(params.Limit).
			SetSort(bson.D{{Key: "updatedAt", Value: -1}})

		cursor, err := r.collection.Find(ctx, filter, opts)
		if err != nil {
			return nil, 0, err
		}
		defer cursor.Close(ctx)

		var products []models.MainProduct
		if err := cursor.All(ctx, &products); err != nil {
			return nil, 0, err
		}
		return products, total, nil
	}

	escapedSearch := regexp.QuoteMeta(strings.TrimSpace(params.Search))
	normalizedSearch := strings.ToLower(strings.TrimSpace(params.Search))
	nameExpr := bson.M{"$toString": bson.M{"$ifNull": bson.A{"$name", ""}}}
	trimmedNameExpr := bson.M{"$trim": bson.M{"input": nameExpr}}
	normalizedNameExpr := bson.M{"$toLower": trimmedNameExpr}

	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: filter}},
		{{Key: "$addFields", Value: bson.M{
			"__searchPriority": bson.M{
				"$switch": bson.M{
					"branches": bson.A{
						bson.M{
							"case": bson.M{
								"$eq": bson.A{
									normalizedNameExpr,
									normalizedSearch,
								},
							},
							"then": 4,
						},
						bson.M{
							"case": bson.M{
								"$regexMatch": bson.M{
									"input":   trimmedNameExpr,
									"regex":   "^" + escapedSearch,
									"options": "i",
								},
							},
							"then": 3,
						},
						bson.M{
							"case": bson.M{
								"$regexMatch": bson.M{
									"input":   trimmedNameExpr,
									"regex":   "(^|\\s)" + escapedSearch,
									"options": "i",
								},
							},
							"then": 2,
						},
						bson.M{
							"case": bson.M{
								"$regexMatch": bson.M{
									"input":   trimmedNameExpr,
									"regex":   escapedSearch,
									"options": "i",
								},
							},
							"then": 1,
						},
					},
					"default": 0,
				},
			},
			"__nameLength": bson.M{
				"$strLenCP": trimmedNameExpr,
			},
			"__nameSearchPosition": bson.M{
				"$indexOfCP": bson.A{
					normalizedNameExpr,
					normalizedSearch,
				},
			},
		}}},
		{{Key: "$addFields", Value: bson.M{
			"__nameSearchPositionSort": bson.M{
				"$cond": bson.M{
					"if":   bson.M{"$gte": bson.A{"$__nameSearchPosition", 0}},
					"then": "$__nameSearchPosition",
					"else": 2147483647,
				},
			},
		}}},
		{{Key: "$sort", Value: bson.D{
			{Key: "__searchPriority", Value: -1},
			{Key: "__nameSearchPositionSort", Value: 1},
			{Key: "__nameLength", Value: 1},
			{Key: "updatedAt", Value: -1},
		}}},
		{{Key: "$skip", Value: (params.Page - 1) * params.Limit}},
		{{Key: "$limit", Value: params.Limit}},
		{{Key: "$project", Value: bson.M{
			"__searchPriority":         0,
			"__nameLength":             0,
			"__nameSearchPosition":     0,
			"__nameSearchPositionSort": 0,
		}}},
	}

	cursor, err := r.collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	var products []models.MainProduct
	if err := cursor.All(ctx, &products); err != nil {
		return nil, 0, err
	}

	return products, total, nil
}

func (r *MainProductRepository) ListAllWithFilters(ctx context.Context, params MainProductFilterParams) ([]models.MainProduct, error) {
	filter := buildMainProductFilter(params)
	opts := options.Find().SetSort(bson.D{{Key: "updatedAt", Value: -1}})

	cursor, err := r.collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var products []models.MainProduct
	if err := cursor.All(ctx, &products); err != nil {
		return nil, err
	}
	return products, nil
}

func (r *MainProductRepository) ListSourceCategoryPaths(ctx context.Context) ([][]string, error) {
	type categoryDoc struct {
		CategoryPath  []string          `bson:"categoryPath"`
		SubjectName   string            `bson:"subjectName"`
		NicheName     string            `bson:"nicheName"`
		SourceGUIDNOM string            `bson:"sourceGuidNom"`
		SourceGUID    string            `bson:"sourceGuid"`
		CoverURL      string            `bson:"coverUrl"`
		Covers        map[string]string `bson:"covers"`
	}

	cursor, err := r.collection.Find(
		ctx,
		bson.M{},
		options.Find().SetProjection(bson.M{
			"_id":           0,
			"categoryPath":  1,
			"subjectName":   1,
			"nicheName":     1,
			"sourceGuidNom": 1,
			"sourceGuid":    1,
			"coverUrl":      1,
			"covers":        1,
		}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	pathsByKey := make(map[string][]string)
	for cursor.Next(ctx) {
		var doc categoryDoc
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		path := mainProductSourceCategoryPath(
			doc.CategoryPath,
			doc.SubjectName,
			doc.NicheName,
			doc.SourceGUIDNOM,
			doc.SourceGUID,
			doc.CoverURL,
			doc.Covers,
		)
		if len(path) == 0 {
			continue
		}

		key := strings.Join(path, "\x1f")
		if _, exists := pathsByKey[key]; exists {
			continue
		}
		pathsByKey[key] = path
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(pathsByKey))
	for key := range pathsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([][]string, 0, len(keys))
	for _, key := range keys {
		path := pathsByKey[key]
		nextPath := make([]string, len(path))
		copy(nextPath, path)
		result = append(result, nextPath)
	}
	return result, nil
}

func (r *MainProductRepository) ListUncategorizedCategoryHints(ctx context.Context) ([][]string, error) {
	type categoryDoc struct {
		CategoryPath  []string          `bson:"categoryPath"`
		SubjectName   string            `bson:"subjectName"`
		NicheName     string            `bson:"nicheName"`
		SourceGUIDNOM string            `bson:"sourceGuidNom"`
		SourceGUID    string            `bson:"sourceGuid"`
		CoverURL      string            `bson:"coverUrl"`
		Covers        map[string]string `bson:"covers"`
	}

	cursor, err := r.collection.Find(
		ctx,
		mainProductWithoutCategoryClause(),
		options.Find().SetProjection(bson.M{
			"_id":           0,
			"categoryPath":  1,
			"subjectName":   1,
			"nicheName":     1,
			"sourceGuidNom": 1,
			"sourceGuid":    1,
			"coverUrl":      1,
			"covers":        1,
		}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	pathsByKey := make(map[string][]string)
	for cursor.Next(ctx) {
		var doc categoryDoc
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		path := mainProductSourceCategoryPath(
			doc.CategoryPath,
			doc.SubjectName,
			doc.NicheName,
			doc.SourceGUIDNOM,
			doc.SourceGUID,
			doc.CoverURL,
			doc.Covers,
		)
		if len(path) == 0 {
			path = []string{"Без категории"}
		}

		key := strings.Join(path, "\x1f")
		if _, exists := pathsByKey[key]; exists {
			continue
		}
		pathsByKey[key] = path
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(pathsByKey))
	for key := range pathsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	result := make([][]string, 0, len(keys))
	for _, key := range keys {
		path := pathsByKey[key]
		nextPath := make([]string, len(path))
		copy(nextPath, path)
		result = append(result, nextPath)
	}
	return result, nil
}

func (r *MainProductRepository) ListSourceDomains(ctx context.Context) ([]string, error) {
	type domainDoc struct {
		SourceGUIDNOM string            `bson:"sourceGuidNom"`
		SourceGUID    string            `bson:"sourceGuid"`
		CoverURL      string            `bson:"coverUrl"`
		Covers        map[string]string `bson:"covers"`
	}

	cursor, err := r.collection.Find(
		ctx,
		bson.M{},
		options.Find().SetProjection(bson.M{
			"_id":           0,
			"sourceGuidNom": 1,
			"sourceGuid":    1,
			"coverUrl":      1,
			"covers":        1,
		}),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	domains := make(map[string]struct{})
	for cursor.Next(ctx) {
		var doc domainDoc
		if err := cursor.Decode(&doc); err != nil {
			continue
		}

		candidates := []string{
			doc.SourceGUIDNOM,
			doc.SourceGUID,
			doc.CoverURL,
		}
		for _, raw := range candidates {
			domain := extractMainProductDomain(raw)
			if domain == "" {
				continue
			}
			domains[domain] = struct{}{}
		}

		for _, raw := range doc.Covers {
			domain := extractMainProductDomain(raw)
			if domain == "" {
				continue
			}
			domains[domain] = struct{}{}
		}
	}
	if err := cursor.Err(); err != nil {
		return nil, err
	}

	result := make([]string, 0, len(domains))
	for domain := range domains {
		result = append(result, domain)
	}
	sort.Strings(result)
	return result, nil
}

func (r *MainProductRepository) ListBillzSyncCandidates(ctx context.Context) ([]MainProductBillzSyncCandidate, error) {
	filter := bson.M{
		"isbn": bson.M{
			"$exists": true,
			"$ne":     "",
		},
	}
	projection := bson.M{
		"_id":            1,
		"isbn":           1,
		"isbnNormalized": 1,
		"quantity":       1,
		"price":          1,
	}

	cursor, err := r.collection.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	candidates := []MainProductBillzSyncCandidate{}
	if err := cursor.All(ctx, &candidates); err != nil {
		return nil, err
	}

	return candidates, nil
}

func (r *MainProductRepository) ApplyBillzSyncUpdates(ctx context.Context, updates []MainProductBillzSyncUpdate) (int, error) {
	if len(updates) == 0 {
		return 0, nil
	}

	now := time.Now().UTC()
	operations := make([]mongo.WriteModel, 0, len(updates))
	for _, item := range updates {
		if item.ID.IsZero() {
			continue
		}

		setDoc := bson.M{
			"quantity":       item.Quantity,
			"price":          item.Price,
			"updatedAt":      now,
			"billzUpdatedAt": now,
		}
		if item.ISBNNormalized != "" {
			setDoc["isbnNormalized"] = item.ISBNNormalized
		}

		operations = append(operations, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": item.ID}).
			SetUpdate(bson.M{"$set": setDoc}))
	}

	if len(operations) == 0 {
		return 0, nil
	}

	result, err := r.collection.BulkWrite(ctx, operations, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, err
	}

	return int(result.ModifiedCount), nil
}

func (r *MainProductRepository) CreateManual(ctx context.Context, product models.MainProduct) (primitive.ObjectID, error) {
	now := time.Now().UTC()
	doc := sanitizeMainProduct(product, now)
	if strings.TrimSpace(doc.Name) == "" {
		return primitive.NilObjectID, errors.New("name is required")
	}
	if doc.ID.IsZero() {
		doc.ID = primitive.NewObjectID()
	}
	doc.CreatedAt = now

	if _, err := r.collection.InsertOne(ctx, doc); err != nil {
		return primitive.NilObjectID, err
	}
	return doc.ID, nil
}

func (r *MainProductRepository) UpdateByID(ctx context.Context, id primitive.ObjectID, product models.MainProduct) (bool, error) {
	if id.IsZero() {
		return false, errors.New("id is required")
	}

	now := time.Now().UTC()
	doc := sanitizeMainProduct(product, now)
	if strings.TrimSpace(doc.Name) == "" {
		return false, errors.New("name is required")
	}

	setDoc := bson.M{
		"name":           doc.Name,
		"quantity":       doc.Quantity,
		"price":          doc.Price,
		"isInfoComplete": doc.IsInfoComplete,
		"updatedAt":      now,
	}
	unsetDoc := bson.M{}

	setOrUnsetString := func(field, value string) {
		if value == "" {
			unsetDoc[field] = ""
			return
		}
		setDoc[field] = value
	}
	setOrUnsetSlice := func(field string, value []string) {
		if len(value) == 0 {
			unsetDoc[field] = ""
			return
		}
		setDoc[field] = value
	}

	setOrUnsetString("isbn", doc.ISBN)
	setOrUnsetString("isbnNormalized", doc.ISBNNormalized)
	setOrUnsetString("authorCover", doc.AuthorCover)
	setOrUnsetSlice("authorNames", doc.AuthorNames)
	if len(doc.AuthorRefs) == 0 {
		unsetDoc["authorRefs"] = ""
	} else {
		setDoc["authorRefs"] = doc.AuthorRefs
	}
	if len(doc.TagRefs) == 0 {
		unsetDoc["tagRefs"] = ""
	} else {
		setDoc["tagRefs"] = doc.TagRefs
	}
	if len(doc.GenreRefs) == 0 {
		unsetDoc["genreRefs"] = ""
	} else {
		setDoc["genreRefs"] = doc.GenreRefs
	}
	setOrUnsetSlice("tagNames", doc.TagNames)
	setOrUnsetSlice("genreNames", doc.GenreNames)
	setOrUnsetString("description", doc.Description)
	setOrUnsetString("annotation", doc.Annotation)
	setOrUnsetString("coverUrl", doc.CoverURL)
	if len(doc.Covers) == 0 {
		unsetDoc["covers"] = ""
	} else {
		setDoc["covers"] = doc.Covers
	}
	if doc.Pages <= 0 {
		unsetDoc["pages"] = ""
	} else {
		setDoc["pages"] = doc.Pages
	}
	setOrUnsetString("format", doc.Format)
	setOrUnsetString("paperType", doc.PaperType)
	setOrUnsetString("bindingType", doc.BindingType)
	setOrUnsetString("ageRestriction", doc.AgeRestriction)
	setOrUnsetString("characteristics", doc.Characteristics)
	setOrUnsetString("boardGameType", doc.BoardGameType)
	setOrUnsetString("productType", doc.ProductType)
	setOrUnsetString("targetAudience", doc.TargetAudience)
	if doc.MinPlayers <= 0 {
		unsetDoc["minPlayers"] = ""
	} else {
		setDoc["minPlayers"] = doc.MinPlayers
	}
	if doc.MaxPlayers <= 0 {
		unsetDoc["maxPlayers"] = ""
	} else {
		setDoc["maxPlayers"] = doc.MaxPlayers
	}
	if doc.MinGameDurationMinutes <= 0 {
		unsetDoc["minGameDurationMinutes"] = ""
	} else {
		setDoc["minGameDurationMinutes"] = doc.MinGameDurationMinutes
	}
	if doc.MaxGameDurationMinutes <= 0 {
		unsetDoc["maxGameDurationMinutes"] = ""
	} else {
		setDoc["maxGameDurationMinutes"] = doc.MaxGameDurationMinutes
	}
	setOrUnsetString("material", doc.Material)
	setOrUnsetString("subjectName", doc.SubjectName)
	setOrUnsetString("nicheName", doc.NicheName)
	setOrUnsetString("brandName", doc.BrandName)
	setOrUnsetString("seriesName", doc.SeriesName)
	if doc.PublicationYear <= 0 {
		unsetDoc["publicationYear"] = ""
	} else {
		setDoc["publicationYear"] = doc.PublicationYear
	}
	setOrUnsetString("productWeight", doc.ProductWeight)
	setOrUnsetString("publisherName", doc.PublisherName)
	setOrUnsetString("sourceGuidNom", doc.SourceGUIDNOM)
	setOrUnsetString("sourceGuid", doc.SourceGUID)
	setOrUnsetString("sourceNomcode", doc.SourceNomCode)
	setOrUnsetSlice("categoryPath", doc.CategoryPath)

	if doc.CategoryID.IsZero() {
		unsetDoc["categoryId"] = ""
	} else {
		setDoc["categoryId"] = doc.CategoryID
	}

	update := bson.M{"$set": setDoc}
	if len(unsetDoc) > 0 {
		update["$unset"] = unsetDoc
	}

	result, err := r.collection.UpdateOne(ctx, bson.M{"_id": id}, update)
	if err != nil {
		return false, err
	}
	return result.MatchedCount > 0, nil
}

func (r *MainProductRepository) UpsertImported(ctx context.Context, products []models.MainProduct) (inserted int, modified int, skipped int, err error) {
	if len(products) == 0 {
		return 0, 0, 0, nil
	}

	for _, item := range products {
		now := time.Now().UTC()
		doc := sanitizeMainProduct(item, now)
		if strings.TrimSpace(doc.Name) == "" {
			skipped++
			continue
		}

		filter, filterKey := buildMainProductImportFilter(doc)
		if filter == nil {
			if doc.ID.IsZero() {
				doc.ID = primitive.NewObjectID()
			}
			doc.CreatedAt = now
			if _, err := r.collection.InsertOne(ctx, doc); err != nil {
				if isDuplicateKeyError(err) {
					skipped++
					continue
				}
				return inserted, modified, skipped, err
			}
			inserted++
			continue
		}

		if filterKey == "" {
			skipped++
			continue
		}

		setDoc := doc
		setDoc.ID = primitive.NilObjectID
		setDoc.CreatedAt = time.Time{}

		result, err := r.collection.UpdateOne(
			ctx,
			filter,
			bson.M{
				"$set": setDoc,
				"$setOnInsert": bson.M{
					"createdAt": now,
				},
			},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			if isDuplicateKeyError(err) {
				skipped++
				continue
			}
			return inserted, modified, skipped, err
		}

		if result.UpsertedCount > 0 {
			inserted++
		} else {
			modified++
		}
	}

	return inserted, modified, skipped, nil
}

func (r *MainProductRepository) DeleteByID(ctx context.Context, id primitive.ObjectID) (*models.MainProduct, bool, error) {
	var deleted models.MainProduct
	err := r.collection.FindOneAndDelete(ctx, bson.M{"_id": id}).Decode(&deleted)
	if err == mongo.ErrNoDocuments {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &deleted, true, nil
}

func (r *MainProductRepository) ListByIDs(ctx context.Context, ids []primitive.ObjectID) ([]models.MainProduct, error) {
	if len(ids) == 0 {
		return []models.MainProduct{}, nil
	}

	cursor, err := r.collection.Find(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var products []models.MainProduct
	if err := cursor.All(ctx, &products); err != nil {
		return nil, err
	}

	return products, nil
}

func (r *MainProductRepository) ListDuplicateScanRecords(ctx context.Context) ([]MainProductDuplicateScanRecord, error) {
	opts := options.Find().SetProjection(bson.M{
		"_id":            1,
		"isbn":           1,
		"isbnNormalized": 1,
		"barcode":        1,
		"code":           1,
		"sourceNomcode":  1,
	})
	cursor, err := r.collection.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var rows []MainProductDuplicateScanRecord
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, err
	}

	return rows, nil
}

func (r *MainProductRepository) DeleteByIDs(ctx context.Context, ids []primitive.ObjectID) ([]models.MainProduct, error) {
	if len(ids) == 0 {
		return []models.MainProduct{}, nil
	}

	filter := bson.M{"_id": bson.M{"$in": ids}}
	projection := bson.M{
		"_id":           1,
		"sourceGuidNom": 1,
		"sourceGuid":    1,
		"sourceNomcode": 1,
	}

	cursor, err := r.collection.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	deleted := []models.MainProduct{}
	if err := cursor.All(ctx, &deleted); err != nil {
		return nil, err
	}
	if len(deleted) == 0 {
		return deleted, nil
	}

	foundIDs := make([]primitive.ObjectID, 0, len(deleted))
	for _, item := range deleted {
		if item.ID.IsZero() {
			continue
		}
		foundIDs = append(foundIDs, item.ID)
	}
	if len(foundIDs) == 0 {
		return []models.MainProduct{}, nil
	}

	if _, err := r.collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": foundIDs}}); err != nil {
		return nil, err
	}

	return deleted, nil
}

func (r *MainProductRepository) DeleteByFilter(ctx context.Context, params MainProductFilterParams) ([]models.MainProduct, error) {
	filter := buildMainProductFilter(params)
	projection := bson.M{
		"_id":           1,
		"sourceGuidNom": 1,
		"sourceGuid":    1,
		"sourceNomcode": 1,
	}

	cursor, err := r.collection.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	deleted := []models.MainProduct{}
	if err := cursor.All(ctx, &deleted); err != nil {
		return nil, err
	}
	if len(deleted) == 0 {
		return deleted, nil
	}

	foundIDs := make([]primitive.ObjectID, 0, len(deleted))
	for _, item := range deleted {
		if item.ID.IsZero() {
			continue
		}
		foundIDs = append(foundIDs, item.ID)
	}
	if len(foundIDs) == 0 {
		return []models.MainProduct{}, nil
	}

	if _, err := r.collection.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": foundIDs}}); err != nil {
		return nil, err
	}

	return deleted, nil
}

func (r *MainProductRepository) RemoveCategoryByID(ctx context.Context, id primitive.ObjectID) (bool, error) {
	result, err := r.collection.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{
			"$unset": bson.M{
				"categoryId":   "",
				"categoryPath": "",
			},
			"$set": bson.M{
				"updatedAt": time.Now().UTC(),
			},
		},
	)
	if err != nil {
		return false, err
	}
	return result.MatchedCount > 0, nil
}

func (r *MainProductRepository) RemoveCategoryByIDs(
	ctx context.Context,
	ids []primitive.ObjectID,
) (matched int64, modified int64, err error) {
	if len(ids) == 0 {
		return 0, 0, nil
	}

	update := bson.M{
		"$unset": bson.M{
			"categoryId":   "",
			"categoryPath": "",
		},
		"$set": bson.M{
			"updatedAt": time.Now().UTC(),
		},
	}

	result, err := r.collection.UpdateMany(
		ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		update,
	)
	if err != nil {
		return 0, 0, err
	}
	return result.MatchedCount, result.ModifiedCount, nil
}

func (r *MainProductRepository) RemoveCategoryByFilter(
	ctx context.Context,
	params MainProductFilterParams,
) (matched int64, modified int64, err error) {
	filter := buildMainProductFilter(params)
	update := bson.M{
		"$unset": bson.M{
			"categoryId":   "",
			"categoryPath": "",
		},
		"$set": bson.M{
			"updatedAt": time.Now().UTC(),
		},
	}

	result, err := r.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, 0, err
	}
	return result.MatchedCount, result.ModifiedCount, nil
}

func (r *MainProductRepository) AssignCategoryByIDs(
	ctx context.Context,
	ids []primitive.ObjectID,
	categoryID primitive.ObjectID,
	categoryPath []string,
) (matched int64, modified int64, err error) {
	if len(ids) == 0 {
		return 0, 0, nil
	}
	if categoryID.IsZero() {
		return 0, 0, errors.New("categoryId is required")
	}

	update := bson.M{
		"$set": bson.M{
			"categoryId":   categoryID,
			"categoryPath": sanitizeStringSlice(categoryPath),
			"updatedAt":    time.Now().UTC(),
		},
	}

	result, err := r.collection.UpdateMany(
		ctx,
		bson.M{"_id": bson.M{"$in": ids}},
		update,
	)
	if err != nil {
		return 0, 0, err
	}
	return result.MatchedCount, result.ModifiedCount, nil
}

func (r *MainProductRepository) AssignCategoryByFilter(
	ctx context.Context,
	params MainProductFilterParams,
	categoryID primitive.ObjectID,
	categoryPath []string,
) (matched int64, modified int64, err error) {
	if categoryID.IsZero() {
		return 0, 0, errors.New("categoryId is required")
	}

	filter := buildMainProductFilter(params)
	update := bson.M{
		"$set": bson.M{
			"categoryId":   categoryID,
			"categoryPath": sanitizeStringSlice(categoryPath),
			"updatedAt":    time.Now().UTC(),
		},
	}

	result, err := r.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		return 0, 0, err
	}
	return result.MatchedCount, result.ModifiedCount, nil
}

func (r *MainProductRepository) RefreshCategoryPathsByCategoryID(
	ctx context.Context,
	pathsByCategoryID map[primitive.ObjectID][]string,
) (matched int64, modified int64, err error) {
	if len(pathsByCategoryID) == 0 {
		return 0, 0, nil
	}

	now := time.Now().UTC()
	operations := make([]mongo.WriteModel, 0, len(pathsByCategoryID))
	for categoryID, path := range pathsByCategoryID {
		if categoryID.IsZero() {
			continue
		}
		update := bson.M{
			"$set": bson.M{
				"categoryPath": sanitizeStringSlice(path),
				"updatedAt":    now,
			},
		}
		operations = append(operations, mongo.NewUpdateManyModel().
			SetFilter(bson.M{"categoryId": categoryID}).
			SetUpdate(update))
	}

	if len(operations) == 0 {
		return 0, 0, nil
	}

	result, err := r.collection.BulkWrite(ctx, operations, options.BulkWrite().SetOrdered(false))
	if err != nil {
		return 0, 0, err
	}
	return result.MatchedCount, result.ModifiedCount, nil
}

func (r *MainProductRepository) RemoveCategoryByCategoryIDs(
	ctx context.Context,
	categoryIDs []primitive.ObjectID,
) (matched int64, modified int64, err error) {
	if len(categoryIDs) == 0 {
		return 0, 0, nil
	}

	unique := make(map[primitive.ObjectID]struct{}, len(categoryIDs))
	ids := make([]primitive.ObjectID, 0, len(categoryIDs))
	for _, id := range categoryIDs {
		if id.IsZero() {
			continue
		}
		if _, exists := unique[id]; exists {
			continue
		}
		unique[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return 0, 0, nil
	}

	update := bson.M{
		"$unset": bson.M{
			"categoryId":   "",
			"categoryPath": "",
		},
		"$set": bson.M{
			"updatedAt": time.Now().UTC(),
		},
	}

	result, err := r.collection.UpdateMany(ctx, bson.M{"categoryId": bson.M{"$in": ids}}, update)
	if err != nil {
		return 0, 0, err
	}
	return result.MatchedCount, result.ModifiedCount, nil
}

func (r *MainProductRepository) ExistsBySource(ctx context.Context, guidNom, guid, nomcode string) (bool, error) {
	filter := bson.M{}
	switch {
	case guidNom != "":
		filter["sourceGuidNom"] = guidNom
	case guid != "":
		filter["sourceGuid"] = guid
	case nomcode != "":
		filter["sourceNomcode"] = nomcode
	default:
		return false, nil
	}

	count, err := r.collection.CountDocuments(ctx, filter)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// ExistsForEksmoProducts returns a bool slice aligned with input products.
// true means the product already exists in main_products by stable source key.
func (r *MainProductRepository) ExistsForEksmoProducts(ctx context.Context, products []models.EksmoProduct) ([]bool, error) {
	if len(products) == 0 {
		return []bool{}, nil
	}

	guidNomSet := map[string]struct{}{}
	guidSet := map[string]struct{}{}
	nomcodeSet := map[string]struct{}{}
	sourceProductIDSet := map[primitive.ObjectID]struct{}{}

	for _, product := range products {
		if product.GUIDNOM != "" {
			guidNomSet[product.GUIDNOM] = struct{}{}
			continue
		}
		if product.GUID != "" {
			guidSet[product.GUID] = struct{}{}
			continue
		}
		if product.NomCode != "" {
			nomcodeSet[product.NomCode] = struct{}{}
			continue
		}
		if !product.ID.IsZero() {
			sourceProductIDSet[product.ID] = struct{}{}
		}
	}

	orFilters := []bson.M{}
	if len(guidNomSet) > 0 {
		orFilters = append(orFilters, bson.M{"sourceGuidNom": bson.M{"$in": setKeys(guidNomSet)}})
	}
	if len(guidSet) > 0 {
		orFilters = append(orFilters, bson.M{"sourceGuid": bson.M{"$in": setKeys(guidSet)}})
	}
	if len(nomcodeSet) > 0 {
		orFilters = append(orFilters, bson.M{"sourceNomcode": bson.M{"$in": setKeys(nomcodeSet)}})
	}
	if len(sourceProductIDSet) > 0 {
		orFilters = append(orFilters, bson.M{"sourceProductId": bson.M{"$in": objectIDSetKeys(sourceProductIDSet)}})
	}

	if len(orFilters) == 0 {
		return make([]bool, len(products)), nil
	}

	filter := bson.M{"$or": orFilters}
	projection := bson.M{
		"_id":             0,
		"sourceProductId": 1,
		"sourceGuidNom":   1,
		"sourceGuid":      1,
		"sourceNomcode":   1,
	}

	cursor, err := r.collection.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	type sourceDoc struct {
		SourceProductID primitive.ObjectID `bson:"sourceProductId"`
		SourceGUIDNOM   string             `bson:"sourceGuidNom"`
		SourceGUID      string             `bson:"sourceGuid"`
		SourceNomcode   string             `bson:"sourceNomcode"`
	}

	existingKeys := make(map[string]struct{}, len(products))
	for cursor.Next(ctx) {
		var doc sourceDoc
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		if doc.SourceGUIDNOM != "" {
			existingKeys["guidNom:"+doc.SourceGUIDNOM] = struct{}{}
		} else if doc.SourceGUID != "" {
			existingKeys["guid:"+doc.SourceGUID] = struct{}{}
		} else if doc.SourceNomcode != "" {
			existingKeys["nomcode:"+doc.SourceNomcode] = struct{}{}
		} else if !doc.SourceProductID.IsZero() {
			existingKeys["sourceProductId:"+doc.SourceProductID.Hex()] = struct{}{}
		}
	}

	result := make([]bool, len(products))
	for i, product := range products {
		key := eksmoStableSourceKey(product)
		if key == "" {
			result[i] = false
			continue
		}
		_, exists := existingKeys[key]
		result[i] = exists
	}

	return result, nil
}

func eksmoStableSourceKey(product models.EksmoProduct) string {
	if product.GUIDNOM != "" {
		return "guidNom:" + product.GUIDNOM
	}
	if product.GUID != "" {
		return "guid:" + product.GUID
	}
	if product.NomCode != "" {
		return "nomcode:" + product.NomCode
	}
	if !product.ID.IsZero() {
		return "sourceProductId:" + product.ID.Hex()
	}
	return ""
}

func setKeys(source map[string]struct{}) []string {
	keys := make([]string, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	return keys
}

func objectIDSetKeys(source map[primitive.ObjectID]struct{}) []primitive.ObjectID {
	keys := make([]primitive.ObjectID, 0, len(source))
	for key := range source {
		keys = append(keys, key)
	}
	return keys
}

func stringSliceToBsonArray(values []string) bson.A {
	if len(values) == 0 {
		return bson.A{}
	}
	result := make(bson.A, 0, len(values))
	for _, value := range values {
		result = append(result, value)
	}
	return result
}

func normalizeBillzISBN(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(trimmed))
	for _, r := range trimmed {
		switch {
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r - ('a' - 'A'))
		}
	}
	return builder.String()
}

func buildMainProductFilter(params MainProductFilterParams) bson.M {
	clauses := make([]bson.M, 0, 2)
	if params.Search != "" {
		search := regexp.QuoteMeta(params.Search)
		clauses = append(clauses, bson.M{
			"$or": []bson.M{
				{"name": bson.M{"$regex": search, "$options": "i"}},
				{"isbn": bson.M{"$regex": search, "$options": "i"}},
				{"authorCover": bson.M{"$regex": search, "$options": "i"}},
				{"authorNames": bson.M{"$regex": search, "$options": "i"}},
				{"sourceGuidNom": bson.M{"$regex": search, "$options": "i"}},
			},
		})
	}
	if params.WithoutISBN {
		clauses = append(clauses, mainProductWithoutISBNClause())
	}
	if params.BillzSyncable != nil {
		clauses = append(clauses, mainProductBillzSyncableClause(*params.BillzSyncable))
	}
	if params.InfoComplete != nil {
		clauses = append(clauses, mainProductInfoCompleteClause(*params.InfoComplete))
	}
	if len(params.ExcludeIDs) > 0 {
		clauses = append(clauses, bson.M{"_id": bson.M{"$nin": params.ExcludeIDs}})
	}

	categoryClauses := make([]bson.M, 0, 2)
	if len(params.CategoryIDs) > 0 {
		categoryClauses = append(categoryClauses, bson.M{"categoryId": bson.M{"$in": params.CategoryIDs}})
	} else if !params.CategoryID.IsZero() {
		categoryClauses = append(categoryClauses, bson.M{"categoryId": params.CategoryID})
	}
	if params.WithoutCategory {
		categoryClauses = append(categoryClauses, mainProductWithoutCategoryClause())
	}
	if params.IncludeEksmoSources {
		categoryClauses = append(categoryClauses, mainProductLikelyEksmoClause())
	}
	if len(params.SourceDomains) > 0 {
		sourceDomainClauses := make([]bson.M, 0, len(params.SourceDomains))
		for _, sourceDomain := range params.SourceDomains {
			clause := mainProductSourceDomainClause(sourceDomain)
			if len(clause) == 0 {
				continue
			}
			sourceDomainClauses = append(sourceDomainClauses, clause)
		}
		if len(sourceDomainClauses) == 1 {
			categoryClauses = append(categoryClauses, sourceDomainClauses[0])
		} else if len(sourceDomainClauses) > 1 {
			categoryClauses = append(categoryClauses, bson.M{"$or": sourceDomainClauses})
		}
	}
	if len(params.SourceCategoryPaths) > 0 {
		sourcePathClauses := make([]bson.M, 0, len(params.SourceCategoryPaths))
		for _, sourcePath := range params.SourceCategoryPaths {
			path := sanitizeStringSlice(sourcePath)
			if len(path) == 0 {
				continue
			}
			if looksLikeMainProductHost(path[0]) {
				domainClause := mainProductSourceDomainClause(path[0])
				if len(domainClause) == 0 {
					continue
				}
				if len(path) == 1 {
					sourcePathClauses = append(sourcePathClauses, domainClause)
					continue
				}

				suffix := sanitizeStringSlice(path[1:])
				if len(suffix) == 0 {
					sourcePathClauses = append(sourcePathClauses, domainClause)
					continue
				}

				sourcePathClauses = append(sourcePathClauses, bson.M{
					"$and": []bson.M{
						domainClause,
						bson.M{
							"$expr": bson.M{
								"$eq": bson.A{
									bson.M{
										"$slice": bson.A{
											mainProductSourceCategoryPathExpr(),
											len(suffix),
										},
									},
									stringSliceToBsonArray(suffix),
								},
							},
						},
					},
				})
				continue
			}

			regexPattern := buildMainProductCoverURLPathRegex(path)
			if regexPattern != "" {
				sourcePathClauses = append(sourcePathClauses, bson.M{
					"coverUrl": bson.M{
						"$regex":   regexPattern,
						"$options": "i",
					},
				})
				continue
			}

			exprClause := bson.M{
				"$expr": bson.M{
					"$eq": bson.A{
						bson.M{
							"$slice": bson.A{
								mainProductSourceCategoryPathExpr(),
								len(path),
							},
						},
						stringSliceToBsonArray(path),
					},
				},
			}
			sourcePathClauses = append(sourcePathClauses, exprClause)
		}

		if len(sourcePathClauses) == 1 {
			categoryClauses = append(categoryClauses, sourcePathClauses[0])
		} else if len(sourcePathClauses) > 1 {
			categoryClauses = append(categoryClauses, bson.M{"$or": sourcePathClauses})
		}
	}
	if len(params.OtherCategoryPaths) > 0 {
		otherPathClauses := make([]bson.M, 0, len(params.OtherCategoryPaths))
		for _, otherPath := range params.OtherCategoryPaths {
			path := sanitizeStringSlice(otherPath)
			if len(path) == 0 {
				continue
			}
			if looksLikeMainProductHost(path[0]) {
				domainClause := mainProductSourceDomainClause(path[0])
				if len(domainClause) == 0 {
					continue
				}
				if len(path) == 1 {
					otherPathClauses = append(otherPathClauses, bson.M{
						"$and": []bson.M{
							mainProductWithoutCategoryClause(),
							domainClause,
						},
					})
					continue
				}

				suffix := sanitizeStringSlice(path[1:])
				if len(suffix) == 0 {
					otherPathClauses = append(otherPathClauses, bson.M{
						"$and": []bson.M{
							mainProductWithoutCategoryClause(),
							domainClause,
						},
					})
					continue
				}

				otherPathClauses = append(otherPathClauses, bson.M{
					"$and": []bson.M{
						mainProductWithoutCategoryClause(),
						domainClause,
						{
							"$expr": bson.M{
								"$eq": bson.A{
									bson.M{
										"$slice": bson.A{
											mainProductOtherCategoryPathExpr(),
											len(suffix),
										},
									},
									stringSliceToBsonArray(suffix),
								},
							},
						},
					},
				})
				continue
			}
			regexPattern := buildMainProductCoverURLPathRegex(path)
			if regexPattern != "" {
				otherPathClauses = append(otherPathClauses, bson.M{
					"$and": []bson.M{
						mainProductWithoutCategoryClause(),
						bson.M{
							"coverUrl": bson.M{
								"$regex":   regexPattern,
								"$options": "i",
							},
						},
					},
				})
				continue
			}

			exprClause := bson.M{
				"$expr": bson.M{
					"$eq": bson.A{
						bson.M{
							"$slice": bson.A{
								mainProductOtherCategoryPathExpr(),
								len(path),
							},
						},
						stringSliceToBsonArray(path),
					},
				},
			}
			otherPathClauses = append(otherPathClauses, bson.M{
				"$and": []bson.M{
					mainProductWithoutCategoryClause(),
					exprClause,
				},
			})
		}

		if len(otherPathClauses) == 1 {
			categoryClauses = append(categoryClauses, otherPathClauses[0])
		} else if len(otherPathClauses) > 1 {
			categoryClauses = append(categoryClauses, bson.M{"$or": otherPathClauses})
		}
	}
	if len(categoryClauses) == 1 {
		clauses = append(clauses, categoryClauses[0])
	} else if len(categoryClauses) > 1 {
		clauses = append(clauses, bson.M{"$or": categoryClauses})
	}

	if len(clauses) == 0 {
		return bson.M{}
	}
	if len(clauses) == 1 {
		return clauses[0]
	}
	return bson.M{"$and": clauses}
}

func mainProductWithoutCategoryClause() bson.M {
	return bson.M{
		"$or": []bson.M{
			{"categoryId": bson.M{"$exists": false}},
			{"categoryId": primitive.NilObjectID},
			{"categoryId": nil},
		},
	}
}

func mainProductWithoutISBNClause() bson.M {
	return bson.M{
		"$or": []bson.M{
			{"isbn": bson.M{"$exists": false}},
			{"isbn": nil},
			{"isbn": bson.M{"$regex": "^\\s*$"}},
		},
	}
}

func mainProductBillzSyncableClause(syncable bool) bson.M {
	if syncable {
		return bson.M{
			"isbn": bson.M{
				"$exists": true,
				"$regex":  "\\S",
			},
		}
	}
	return mainProductWithoutISBNClause()
}

func mainProductInfoCompleteClause(complete bool) bson.M {
	if complete {
		return bson.M{"isInfoComplete": true}
	}
	return bson.M{
		"$or": []bson.M{
			{"isInfoComplete": bson.M{"$exists": false}},
			{"isInfoComplete": nil},
			{"isInfoComplete": false},
		},
	}
}

func mainProductLikelyEksmoClause() bson.M {
	return bson.M{
		"sourceGuidNom": bson.M{
			"$type": "string",
			"$ne":   "",
			"$not":  primitive.Regex{Pattern: "^https?://", Options: "i"},
		},
	}
}

func mainProductSourceDomainClause(domain string) bson.M {
	pattern := buildMainProductSourceDomainRegex(domain)
	if pattern == "" {
		return bson.M{}
	}
	return bson.M{
		"$or": []bson.M{
			{"sourceGuidNom": bson.M{"$regex": pattern, "$options": "i"}},
			{"sourceGuid": bson.M{"$regex": pattern, "$options": "i"}},
			{
				"$and": []bson.M{
					{
						"$nor": []bson.M{
							mainProductFieldHasDomainSourceClause("sourceGuidNom"),
							mainProductFieldHasDomainSourceClause("sourceGuid"),
						},
					},
					{"coverUrl": bson.M{"$regex": pattern, "$options": "i"}},
				},
			},
		},
	}
}

func buildMainProductSourceDomainRegex(domain string) string {
	trimmed := strings.TrimSpace(strings.ToLower(domain))
	if trimmed == "" {
		return ""
	}
	trimmed = strings.TrimPrefix(trimmed, "www.")
	if !looksLikeMainProductHost(trimmed) {
		return ""
	}
	escaped := regexp.QuoteMeta(trimmed)
	// Match exact domain in:
	// - client identifiers (e.g. client:asaxiy.uz:T74018)
	// - URLs/hosts (e.g. https://asaxiy.uz/... or asaxiy.uz/...)
	// Intentionally avoids suffix matching (e.g. azbooka.ru should not match api.azbooka.ru).
	clientPattern := "^client:(?:www\\.)?" + escaped + "(?::|$)"
	urlPattern := "^(?:https?://)?(?:www\\.)?" + escaped + "(?::\\d+)?(?:/|$)"
	return "(?:" + clientPattern + ")|(?:" + urlPattern + ")"
}

func mainProductFieldHasDomainSourceClause(field string) bson.M {
	return bson.M{
		field: bson.M{
			"$regex":   `^(?:https?://|client:(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?::|$)|(?:www\.)?[a-z0-9-]+(?:\.[a-z0-9-]+)+(?::\d+)?(?:/|$))`,
			"$options": "i",
		},
	}
}

func mainProductOtherCategoryPathExpr() bson.M {
	categoryPathExpr := bson.M{"$ifNull": bson.A{"$categoryPath", bson.A{}}}
	subjectNicheExpr := bson.M{
		"$filter": bson.M{
			"input": bson.A{
				bson.M{"$ifNull": bson.A{"$subjectName", ""}},
				bson.M{"$ifNull": bson.A{"$nicheName", ""}},
			},
			"as":   "item",
			"cond": bson.M{"$ne": bson.A{"$$item", ""}},
		},
	}
	return bson.M{
		"$cond": bson.A{
			bson.M{"$gt": bson.A{bson.M{"$size": categoryPathExpr}, 0}},
			categoryPathExpr,
			bson.M{
				"$cond": bson.A{
					bson.M{"$gt": bson.A{bson.M{"$size": subjectNicheExpr}, 0}},
					subjectNicheExpr,
					bson.A{"Без категории"},
				},
			},
		},
	}
}

func mainProductSourceCategoryPathExpr() bson.M {
	subjectNicheExpr := bson.M{
		"$filter": bson.M{
			"input": bson.A{
				bson.M{"$ifNull": bson.A{"$subjectName", ""}},
				bson.M{"$ifNull": bson.A{"$nicheName", ""}},
			},
			"as":   "item",
			"cond": bson.M{"$ne": bson.A{"$$item", ""}},
		},
	}
	categoryPathExpr := bson.M{"$ifNull": bson.A{"$categoryPath", bson.A{}}}
	return bson.M{
		"$cond": bson.A{
			bson.M{"$gt": bson.A{bson.M{"$size": subjectNicheExpr}, 0}},
			subjectNicheExpr,
			bson.M{
				"$cond": bson.A{
					bson.M{"$gt": bson.A{bson.M{"$size": categoryPathExpr}, 0}},
					categoryPathExpr,
					bson.A{},
				},
			},
		},
	}
}

func mainProductSourceCategoryPath(
	categoryPath []string,
	subjectName, nicheName, sourceGUIDNOM, sourceGUID, coverURL string,
	covers map[string]string,
) []string {
	basePath := sanitizeStringSlice([]string{subjectName, nicheName})
	if len(basePath) == 0 {
		basePath = sanitizeStringSlice(categoryPath)
	}

	domain := extractMainProductDomain(sourceGUIDNOM)
	if domain == "" {
		domain = extractMainProductDomain(sourceGUID)
	}
	if domain == "" {
		domain = extractMainProductDomain(firstMainProductCoverURL(coverURL, covers))
	}

	if domain != "" {
		if len(basePath) > 0 {
			result := make([]string, 0, len(basePath)+1)
			result = append(result, domain)
			result = append(result, basePath...)
			return result
		}
		return []string{domain}
	}
	return nil
}

func firstMainProductCoverURL(coverURL string, covers map[string]string) string {
	if trimmed := strings.TrimSpace(coverURL); trimmed != "" {
		return trimmed
	}
	if len(covers) == 0 {
		return ""
	}

	keys := make([]string, 0, len(covers))
	for key := range covers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := strings.TrimSpace(covers[key])
		if value != "" {
			return value
		}
	}
	return ""
}

func extractMainProductURLCategoryPath(rawURL string) []string {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return nil
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil
	}

	host := strings.TrimSpace(strings.ToLower(parsed.Hostname()))
	if host == "" {
		return nil
	}
	host = strings.TrimPrefix(host, "www.")

	pathSegments := strings.Split(strings.Trim(parsed.EscapedPath(), "/"), "/")
	cleanedSegments := make([]string, 0, len(pathSegments))
	for index, rawSegment := range pathSegments {
		segment := strings.TrimSpace(strings.ToLower(rawSegment))
		if segment == "" {
			continue
		}
		if index == len(pathSegments)-1 && looksLikeImageFilenameSegment(segment) {
			continue
		}
		if isMainProductIgnoredURLSegment(segment) {
			continue
		}
		cleanedSegments = append(cleanedSegments, segment)
		if len(cleanedSegments) >= 5 {
			break
		}
	}

	result := make([]string, 0, 1+len(cleanedSegments))
	result = append(result, host)
	result = append(result, cleanedSegments...)
	return result
}

func extractMainProductDomain(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	lowerTrimmed := strings.ToLower(trimmed)

	if strings.HasPrefix(lowerTrimmed, "client:") {
		parts := strings.SplitN(trimmed, ":", 3)
		if len(parts) >= 2 {
			host := strings.TrimSpace(strings.ToLower(parts[1]))
			host = strings.TrimPrefix(host, "www.")
			if looksLikeMainProductHost(host) {
				return host
			}
		}
	}

	if strings.HasPrefix(trimmed, "//") {
		trimmed = "https:" + trimmed
	}

	parseCandidate := trimmed
	if !strings.HasPrefix(lowerTrimmed, "http://") && !strings.HasPrefix(lowerTrimmed, "https://") {
		if looksLikeMainProductHost(parseCandidate) {
			parseCandidate = "https://" + parseCandidate
		} else {
			return ""
		}
	}

	parsed, err := url.Parse(parseCandidate)
	if err != nil {
		return ""
	}
	host := strings.TrimSpace(strings.ToLower(parsed.Hostname()))
	host = strings.TrimPrefix(host, "www.")
	if !looksLikeMainProductHost(host) {
		return ""
	}
	return host
}

func isLikelyExternalMainSourceIdentifier(guidNom, guid string) bool {
	check := []string{guidNom, guid}
	for _, raw := range check {
		value := strings.TrimSpace(strings.ToLower(raw))
		if value == "" {
			continue
		}
		if strings.HasPrefix(value, "client:") || strings.HasPrefix(value, "client_url:") {
			return true
		}
	}
	return false
}

func looksLikeMainProductHost(value string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return false
	}
	return regexp.MustCompile(`^[a-z0-9-]+(\.[a-z0-9-]+)+$`).MatchString(trimmed)
}

func looksLikeImageFilenameSegment(segment string) bool {
	if !strings.Contains(segment, ".") {
		return false
	}
	lower := strings.ToLower(segment)
	extensions := []string{
		".jpg",
		".jpeg",
		".png",
		".webp",
		".gif",
		".bmp",
		".svg",
		".avif",
		".jfif",
	}
	for _, ext := range extensions {
		if strings.HasSuffix(lower, ext) {
			return true
		}
	}
	return false
}

func isMainProductIgnoredURLSegment(segment string) bool {
	if segment == "" {
		return true
	}
	ignored := map[string]struct{}{
		"image":      {},
		"images":     {},
		"img":        {},
		"upload":     {},
		"uploads":    {},
		"media":      {},
		"static":     {},
		"assets":     {},
		"files":      {},
		"file":       {},
		"cache":      {},
		"resized":    {},
		"resize":     {},
		"thumbnail":  {},
		"thumbnails": {},
		"thumb":      {},
		"thumbs":     {},
		"goods":      {},
		"products":   {},
		"product":    {},
	}
	_, exists := ignored[segment]
	return exists
}

func buildMainProductCoverURLPathRegex(path []string) string {
	safePath := sanitizeStringSlice(path)
	if len(safePath) == 0 {
		return ""
	}

	host := strings.TrimSpace(strings.ToLower(safePath[0]))
	if host == "" || !strings.Contains(host, ".") {
		return ""
	}

	hostPattern := regexp.QuoteMeta(strings.TrimPrefix(host, "www."))
	if len(safePath) == 1 {
		return "^https?://(?:www\\.)?" + hostPattern + "(?::\\d+)?(?:/|$)"
	}

	pattern := "^https?://(?:www\\.)?" + hostPattern + "(?::\\d+)?/"
	for index := 1; index < len(safePath); index++ {
		segment := strings.TrimSpace(strings.ToLower(safePath[index]))
		if segment == "" {
			continue
		}
		quoted := regexp.QuoteMeta(segment)
		if index == len(safePath)-1 {
			pattern += "(?:[^/?#]+/)*" + quoted + "(?:/|\\?|#|$)"
		} else {
			pattern += "(?:[^/?#]+/)*" + quoted + "/"
		}
	}
	return pattern
}

func sanitizeMainProduct(product models.MainProduct, now time.Time) models.MainProduct {
	product.Name = strings.TrimSpace(product.Name)
	product.ISBN = strings.TrimSpace(product.ISBN)
	product.ISBNNormalized = normalizeBillzISBN(product.ISBN)
	product.SourceGUID = strings.TrimSpace(product.SourceGUID)
	product.SourceGUIDNOM = strings.TrimSpace(product.SourceGUIDNOM)
	product.SourceNomCode = strings.TrimSpace(product.SourceNomCode)
	product.AuthorCover = strings.TrimSpace(product.AuthorCover)
	product.Description = strings.TrimSpace(product.Description)
	product.Annotation = strings.TrimSpace(product.Annotation)
	if product.Description == "" && product.Annotation != "" {
		product.Description = product.Annotation
	}
	if product.Annotation == "" && product.Description != "" {
		product.Annotation = product.Description
	}
	product.AuthorNames = sanitizeStringSlice(product.AuthorNames)
	product.AuthorRefs = sanitizeMainProductAuthorRefs(product.AuthorRefs)
	product.TagRefs = sanitizeMainProductTagRefs(product.TagRefs)
	product.GenreRefs = sanitizeMainProductGenreRefs(product.GenreRefs)
	product.TagNames = sanitizeStringSlice(product.TagNames)
	product.GenreNames = sanitizeStringSlice(product.GenreNames)
	product.CoverURL = strings.TrimSpace(product.CoverURL)
	product.Covers = sanitizeCoverMap(product.Covers)
	if product.CoverURL == "" {
		product.CoverURL = firstCoverURL(product.Covers)
	}
	if product.CoverURL != "" && !containsCoverURL(product.Covers, product.CoverURL) {
		if product.Covers == nil {
			product.Covers = map[string]string{}
		}
		product.Covers["manual_"+strconv.Itoa(len(product.Covers)+1)] = product.CoverURL
	}
	if product.Pages < 0 {
		product.Pages = 0
	}
	product.Format = strings.TrimSpace(product.Format)
	product.PaperType = strings.TrimSpace(product.PaperType)
	product.BindingType = strings.TrimSpace(product.BindingType)
	product.AgeRestriction = strings.TrimSpace(product.AgeRestriction)
	product.Characteristics = strings.TrimSpace(product.Characteristics)
	product.BoardGameType = strings.TrimSpace(product.BoardGameType)
	product.ProductType = strings.TrimSpace(product.ProductType)
	product.TargetAudience = strings.TrimSpace(product.TargetAudience)
	if product.MinPlayers < 0 {
		product.MinPlayers = 0
	}
	if product.MaxPlayers < 0 {
		product.MaxPlayers = 0
	}
	if product.MinGameDurationMinutes < 0 {
		product.MinGameDurationMinutes = 0
	}
	if product.MaxGameDurationMinutes < 0 {
		product.MaxGameDurationMinutes = 0
	}
	product.Material = strings.TrimSpace(product.Material)
	product.SubjectName = strings.TrimSpace(product.SubjectName)
	product.NicheName = strings.TrimSpace(product.NicheName)
	product.BrandName = strings.TrimSpace(product.BrandName)
	product.SeriesName = strings.TrimSpace(product.SeriesName)
	if product.PublicationYear < 0 {
		product.PublicationYear = 0
	}
	product.ProductWeight = strings.TrimSpace(product.ProductWeight)
	product.PublisherName = strings.TrimSpace(product.PublisherName)
	product.CategoryPath = sanitizeStringSlice(product.CategoryPath)
	product.UpdatedAt = now
	return product
}

func sanitizeMainProductAuthorRefs(values []models.EksmoProductAuthorRef) []models.EksmoProductAuthorRef {
	if len(values) == 0 {
		return nil
	}
	result := make([]models.EksmoProductAuthorRef, 0, len(values))
	for _, item := range values {
		item.GUID = strings.TrimSpace(item.GUID)
		item.Code = strings.TrimSpace(item.Code)
		item.Name = strings.TrimSpace(item.Name)
		if item.GUID == "" && item.Name == "" && item.Code == "" {
			continue
		}
		result = append(result, item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func sanitizeMainProductTagRefs(values []models.EksmoProductTagRef) []models.EksmoProductTagRef {
	if len(values) == 0 {
		return nil
	}
	result := make([]models.EksmoProductTagRef, 0, len(values))
	for _, item := range values {
		item.GUID = strings.TrimSpace(item.GUID)
		item.Name = strings.TrimSpace(item.Name)
		if item.GUID == "" && item.Name == "" {
			continue
		}
		result = append(result, item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func sanitizeMainProductGenreRefs(values []models.EksmoProductGenreRef) []models.EksmoProductGenreRef {
	if len(values) == 0 {
		return nil
	}
	result := make([]models.EksmoProductGenreRef, 0, len(values))
	for _, item := range values {
		item.GUID = strings.TrimSpace(item.GUID)
		item.Name = strings.TrimSpace(item.Name)
		if item.GUID == "" && item.Name == "" {
			continue
		}
		result = append(result, item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func sanitizeCoverMap(covers map[string]string) map[string]string {
	if len(covers) == 0 {
		return nil
	}

	type pair struct {
		key string
		url string
	}
	pairs := make([]pair, 0, len(covers))
	for key, value := range covers {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		pairs = append(pairs, pair{key: key, url: value})
	}
	if len(pairs) == 0 {
		return nil
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].key < pairs[j].key
	})

	result := make(map[string]string, len(pairs))
	seen := map[string]struct{}{}
	index := 1
	for _, item := range pairs {
		if _, exists := seen[item.url]; exists {
			continue
		}
		seen[item.url] = struct{}{}
		result["manual_"+strconv.Itoa(index)] = item.url
		index++
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func firstCoverURL(covers map[string]string) string {
	if len(covers) == 0 {
		return ""
	}
	keys := make([]string, 0, len(covers))
	for key := range covers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := strings.TrimSpace(covers[key])
		if value != "" {
			return value
		}
	}
	return ""
}

func containsCoverURL(covers map[string]string, url string) bool {
	url = strings.TrimSpace(url)
	if len(covers) == 0 || url == "" {
		return false
	}
	for _, value := range covers {
		if strings.TrimSpace(value) == url {
			return true
		}
	}
	return false
}

func buildMainProductImportFilter(product models.MainProduct) (bson.M, string) {
	if !product.ID.IsZero() {
		return bson.M{"_id": product.ID}, "id:" + product.ID.Hex()
	}
	if product.SourceGUIDNOM != "" {
		return bson.M{"sourceGuidNom": product.SourceGUIDNOM}, "sourceGuidNom:" + product.SourceGUIDNOM
	}
	if product.SourceGUID != "" {
		return bson.M{"sourceGuid": product.SourceGUID}, "sourceGuid:" + product.SourceGUID
	}
	if product.SourceNomCode != "" {
		return bson.M{"sourceNomcode": product.SourceNomCode}, "sourceNomcode:" + product.SourceNomCode
	}
	if !product.SourceProductID.IsZero() {
		return bson.M{"sourceProductId": product.SourceProductID}, "sourceProductId:" + product.SourceProductID.Hex()
	}
	if product.ISBNNormalized != "" && product.Name != "" {
		return bson.M{
			"isbnNormalized": product.ISBNNormalized,
			"name":           product.Name,
		}, "isbnNormalized+name:" + product.ISBNNormalized + ":" + product.Name
	}
	return nil, ""
}

func isDuplicateKeyError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "E11000")
}

func extractEksmoPrice(raw bson.M) (float64, bool) {
	if len(raw) == 0 {
		return 0, false
	}

	for _, key := range []string{"COST", "PRICE", "RETAIL_PRICE", "BASE_PRICE", "PRICE_RUB"} {
		if value, ok := parsePositiveFloat(raw[key]); ok {
			return value, true
		}
	}

	pricesValue, exists := raw["PRICES"]
	if !exists {
		return 0, false
	}
	return extractEksmoPriceFromPrices(pricesValue)
}

func extractEksmoPriceFromPrices(value any) (float64, bool) {
	switch typed := value.(type) {
	case map[string]any:
		for _, key := range []string{"COST", "PRICE", "RETAIL", "BASE"} {
			if parsed, ok := parsePositiveFloat(typed[key]); ok {
				return parsed, true
			}
		}
		if parsed, ok := extractEksmoPriceFromTypes(typed["TYPES"]); ok {
			return parsed, true
		}
	case bson.M:
		return extractEksmoPriceFromPrices(map[string]any(typed))
	}
	return 0, false
}

func extractEksmoPriceFromTypes(value any) (float64, bool) {
	types, ok := value.([]any)
	if !ok || len(types) == 0 {
		return 0, false
	}

	for _, typeValue := range types {
		item, ok := typeValue.(map[string]any)
		if !ok {
			if asBson, ok := typeValue.(bson.M); ok {
				item = map[string]any(asBson)
			} else {
				continue
			}
		}

		for _, key := range []string{"PRICE", "COST", "VALUE", "SUM"} {
			if parsed, ok := parsePositiveFloat(item[key]); ok {
				return parsed, true
			}
		}

		for key, fieldValue := range item {
			keyLower := strings.ToLower(strings.TrimSpace(key))
			if strings.Contains(keyLower, "price") || strings.Contains(keyLower, "cost") || strings.Contains(keyLower, "sum") {
				if parsed, ok := parsePositiveFloat(fieldValue); ok {
					return parsed, true
				}
			}
		}
	}

	return 0, false
}

func parsePositiveFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		if typed > 0 {
			return typed, true
		}
	case float32:
		if typed > 0 {
			return float64(typed), true
		}
	case int:
		if typed > 0 {
			return float64(typed), true
		}
	case int64:
		if typed > 0 {
			return float64(typed), true
		}
	case int32:
		if typed > 0 {
			return float64(typed), true
		}
	case string:
		normalized := strings.TrimSpace(strings.ReplaceAll(typed, ",", "."))
		if normalized == "" {
			return 0, false
		}
		parsed, err := strconv.ParseFloat(normalized, 64)
		if err == nil && parsed > 0 {
			return parsed, true
		}
	}
	return 0, false
}

func sanitizeStringSlice(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	result := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
