package repository

import (
	"context"
	"errors"
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
	if categoryID.IsZero() {
		return 0, 0, 0, errors.New("categoryId is required")
	}

	now := time.Now().UTC()
	operations := make([]mongo.WriteModel, 0, len(products))
	seen := make(map[string]struct{}, len(products))

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
			Annotation:      p.Annotation,
			CoverURL:        p.CoverURL,
			AgeRestriction:  p.AgeRestriction,
			SubjectName:     productSubjectName(p),
			NicheName:       productNicheName(p),
			BrandName:       productBrandName(p),
			SeriesName:      productSeriesName(p),
			PublisherName:   productPublisherName(p),
			CategoryID:      categoryID,
			CategoryPath:    append([]string{}, categoryPath...),
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

type MainProductFilterParams struct {
	Page        int64
	Limit       int64
	Search      string
	CategoryID  primitive.ObjectID
	CategoryIDs []primitive.ObjectID
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
		"name":      doc.Name,
		"quantity":  doc.Quantity,
		"price":     doc.Price,
		"updatedAt": now,
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
	setOrUnsetString("annotation", doc.Annotation)
	setOrUnsetString("coverUrl", doc.CoverURL)
	if len(doc.Covers) == 0 {
		unsetDoc["covers"] = ""
	} else {
		setDoc["covers"] = doc.Covers
	}
	setOrUnsetString("ageRestriction", doc.AgeRestriction)
	setOrUnsetString("subjectName", doc.SubjectName)
	setOrUnsetString("nicheName", doc.NicheName)
	setOrUnsetString("brandName", doc.BrandName)
	setOrUnsetString("seriesName", doc.SeriesName)
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
	filter := bson.M{}

	if params.Search != "" {
		search := regexp.QuoteMeta(params.Search)
		filter["$or"] = []bson.M{
			{"name": bson.M{"$regex": search, "$options": "i"}},
			{"isbn": bson.M{"$regex": search, "$options": "i"}},
			{"authorCover": bson.M{"$regex": search, "$options": "i"}},
			{"authorNames": bson.M{"$regex": search, "$options": "i"}},
			{"sourceGuidNom": bson.M{"$regex": search, "$options": "i"}},
		}
	}

	if len(params.CategoryIDs) > 0 {
		filter["categoryId"] = bson.M{"$in": params.CategoryIDs}
	} else if !params.CategoryID.IsZero() {
		filter["categoryId"] = params.CategoryID
	}

	return filter
}

func sanitizeMainProduct(product models.MainProduct, now time.Time) models.MainProduct {
	product.Name = strings.TrimSpace(product.Name)
	product.ISBN = strings.TrimSpace(product.ISBN)
	product.ISBNNormalized = normalizeBillzISBN(product.ISBN)
	product.SourceGUID = strings.TrimSpace(product.SourceGUID)
	product.SourceGUIDNOM = strings.TrimSpace(product.SourceGUIDNOM)
	product.SourceNomCode = strings.TrimSpace(product.SourceNomCode)
	product.AuthorCover = strings.TrimSpace(product.AuthorCover)
	product.Annotation = strings.TrimSpace(product.Annotation)
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
	product.AgeRestriction = strings.TrimSpace(product.AgeRestriction)
	product.SubjectName = strings.TrimSpace(product.SubjectName)
	product.NicheName = strings.TrimSpace(product.NicheName)
	product.BrandName = strings.TrimSpace(product.BrandName)
	product.SeriesName = strings.TrimSpace(product.SeriesName)
	product.PublisherName = strings.TrimSpace(product.PublisherName)
	product.AuthorNames = sanitizeStringSlice(product.AuthorNames)
	product.CategoryPath = sanitizeStringSlice(product.CategoryPath)
	product.UpdatedAt = now
	return product
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
