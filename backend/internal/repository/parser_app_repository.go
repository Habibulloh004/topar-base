package repository

import (
	"context"
	"errors"
	"sort"
	"time"

	"topar/backend/internal/models"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ParserAppRepository struct {
	runs     *mongo.Collection
	records  *mongo.Collection
	mappings *mongo.Collection
}

func NewParserAppRepository(db *mongo.Database) *ParserAppRepository {
	return &ParserAppRepository{
		runs:     db.Collection("parser_runs"),
		records:  db.Collection("parser_records"),
		mappings: db.Collection("parser_mappings"),
	}
}

func (r *ParserAppRepository) EnsureIndexes(ctx context.Context) error {
	if r == nil {
		return errors.New("parser repository is nil")
	}

	runIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "createdAt", Value: -1}},
			Options: options.Index().SetName("createdAt_desc_idx"),
		},
		{
			Keys:    bson.D{{Key: "status", Value: 1}, {Key: "createdAt", Value: -1}},
			Options: options.Index().SetName("status_createdAt_idx"),
		},
	}
	if _, err := r.runs.Indexes().CreateMany(ctx, runIndexes); err != nil {
		return err
	}

	recordIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "runId", Value: 1}, {Key: "createdAt", Value: 1}},
			Options: options.Index().SetName("runId_createdAt_idx"),
		},
		{
			Keys:    bson.D{{Key: "runId", Value: 1}, {Key: "sourceUrl", Value: 1}},
			Options: options.Index().SetName("runId_sourceUrl_idx"),
		},
	}
	if _, err := r.records.Indexes().CreateMany(ctx, recordIndexes); err != nil {
		return err
	}

	mappingIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "name", Value: 1}},
			Options: options.Index().SetName("name_idx"),
		},
		{
			Keys:    bson.D{{Key: "updatedAt", Value: -1}},
			Options: options.Index().SetName("updatedAt_desc_idx"),
		},
	}
	_, err := r.mappings.Indexes().CreateMany(ctx, mappingIndexes)
	return err
}

func (r *ParserAppRepository) CreateRun(ctx context.Context, run models.ParserRun) (models.ParserRun, error) {
	now := time.Now().UTC()
	run.ID = primitive.NilObjectID
	run.CreatedAt = now
	if run.Status == "" {
		run.Status = models.ParserRunStatusRunning
	}
	result, err := r.runs.InsertOne(ctx, run)
	if err != nil {
		return models.ParserRun{}, err
	}
	id, ok := result.InsertedID.(primitive.ObjectID)
	if !ok {
		return models.ParserRun{}, errors.New("inserted parser run id has invalid type")
	}
	run.ID = id
	return run, nil
}

func (r *ParserAppRepository) FinishRun(
	ctx context.Context,
	runID primitive.ObjectID,
	detectedFields []string,
	discoveredURLs int,
	parsedProducts int,
	rateLimitRetries int,
	errMessage string,
) error {
	now := time.Now().UTC()
	status := models.ParserRunStatusFinished
	if errMessage != "" {
		status = models.ParserRunStatusFailed
	}

	update := bson.M{
		"$set": bson.M{
			"status":           status,
			"error":            errMessage,
			"detectedFields":   sanitizeFieldNames(detectedFields),
			"discoveredUrls":   discoveredURLs,
			"parsedProducts":   parsedProducts,
			"rateLimitRetries": rateLimitRetries,
			"finishedAt":       now,
		},
	}
	_, err := r.runs.UpdateByID(ctx, runID, update)
	return err
}

func (r *ParserAppRepository) ReplaceRunRecords(
	ctx context.Context,
	runID primitive.ObjectID,
	records []models.ParserRecord,
) error {
	if runID.IsZero() {
		return errors.New("run id is required")
	}

	if _, err := r.records.DeleteMany(ctx, bson.M{"runId": runID}); err != nil {
		return err
	}
	return r.AppendRunRecords(ctx, runID, records)
}

func (r *ParserAppRepository) AppendRunRecords(
	ctx context.Context,
	runID primitive.ObjectID,
	records []models.ParserRecord,
) error {
	if runID.IsZero() {
		return errors.New("run id is required")
	}
	if len(records) == 0 {
		return nil
	}

	now := time.Now().UTC()
	const batchSize = 500
	for start := 0; start < len(records); start += batchSize {
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		docs := make([]any, 0, end-start)
		for _, item := range records[start:end] {
			item.ID = primitive.NilObjectID
			item.RunID = runID
			if item.CreatedAt.IsZero() {
				item.CreatedAt = now
			}
			docs = append(docs, item)
		}
		if _, err := r.records.InsertMany(ctx, docs, options.InsertMany().SetOrdered(false)); err != nil {
			return err
		}
	}
	return nil
}

func (r *ParserAppRepository) GetRun(ctx context.Context, runID primitive.ObjectID) (models.ParserRun, bool, error) {
	var run models.ParserRun
	err := r.runs.FindOne(ctx, bson.M{"_id": runID}).Decode(&run)
	if err == mongo.ErrNoDocuments {
		return models.ParserRun{}, false, nil
	}
	if err != nil {
		return models.ParserRun{}, false, err
	}
	return run, true, nil
}

func (r *ParserAppRepository) ListRuns(ctx context.Context, limit int64) ([]models.ParserRun, error) {
	if limit < 1 {
		limit = 20
	}
	if limit > 200 {
		limit = 200
	}

	cursor, err := r.runs.Find(ctx, bson.M{}, options.Find().SetSort(bson.D{{Key: "createdAt", Value: -1}}).SetLimit(limit))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	runs := []models.ParserRun{}
	if err := cursor.All(ctx, &runs); err != nil {
		return nil, err
	}
	return runs, nil
}

func (r *ParserAppRepository) CountRunRecords(ctx context.Context, runID primitive.ObjectID) (int64, error) {
	return r.records.CountDocuments(ctx, bson.M{"runId": runID})
}

func (r *ParserAppRepository) ListRunRecords(
	ctx context.Context,
	runID primitive.ObjectID,
	page int64,
	limit int64,
) ([]models.ParserRecord, int64, error) {
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 20
	}
	if limit > 500 {
		limit = 500
	}

	filter := bson.M{"runId": runID}
	total, err := r.records.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}

	cursor, err := r.records.Find(
		ctx,
		filter,
		options.Find().
			SetSort(bson.D{{Key: "createdAt", Value: 1}}).
			SetSkip((page-1)*limit).
			SetLimit(limit),
	)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)

	records := []models.ParserRecord{}
	if err := cursor.All(ctx, &records); err != nil {
		return nil, 0, err
	}

	return records, total, nil
}

func (r *ParserAppRepository) StreamRunRecords(ctx context.Context, runID primitive.ObjectID, batchSize int32) (*mongo.Cursor, error) {
	if batchSize < 1 {
		batchSize = 300
	}
	return r.records.Find(
		ctx,
		bson.M{"runId": runID},
		options.Find().
			SetSort(bson.D{{Key: "createdAt", Value: 1}}).
			SetBatchSize(batchSize),
	)
}

func (r *ParserAppRepository) SaveMappingProfile(
	ctx context.Context,
	name string,
	rules map[string]models.ParserFieldRule,
) (models.ParserMappingProfile, error) {
	now := time.Now().UTC()
	profile := models.ParserMappingProfile{
		ID:        primitive.NewObjectID(),
		Name:      name,
		Rules:     normalizeRules(rules),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if profile.Name == "" {
		profile.Name = "mapping-" + now.Format("20060102-150405")
	}

	if _, err := r.mappings.InsertOne(ctx, profile); err != nil {
		return models.ParserMappingProfile{}, err
	}
	return profile, nil
}

func (r *ParserAppRepository) ListMappingProfiles(ctx context.Context, limit int64) ([]models.ParserMappingProfile, error) {
	if limit < 1 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	cursor, err := r.mappings.Find(
		ctx,
		bson.M{},
		options.Find().SetSort(bson.D{{Key: "updatedAt", Value: -1}}).SetLimit(limit),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	profiles := []models.ParserMappingProfile{}
	if err := cursor.All(ctx, &profiles); err != nil {
		return nil, err
	}
	return profiles, nil
}

func (r *ParserAppRepository) DetectRunFields(ctx context.Context, runID primitive.ObjectID, sampleLimit int64) ([]string, error) {
	if sampleLimit < 1 {
		sampleLimit = 300
	}
	if sampleLimit > 5000 {
		sampleLimit = 5000
	}

	cursor, err := r.records.Find(
		ctx,
		bson.M{"runId": runID},
		options.Find().
			SetProjection(bson.M{"data": 1}).
			SetSort(bson.D{{Key: "createdAt", Value: 1}}).
			SetLimit(sampleLimit),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	fieldSet := map[string]struct{}{}
	for cursor.Next(ctx) {
		var doc struct {
			Data map[string]any `bson:"data"`
		}
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		for key := range doc.Data {
			if key == "" {
				continue
			}
			fieldSet[key] = struct{}{}
		}
	}

	fields := make([]string, 0, len(fieldSet))
	for key := range fieldSet {
		fields = append(fields, key)
	}
	sort.Strings(fields)
	return fields, nil
}

func sanitizeFieldNames(fields []string) []string {
	if len(fields) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	result := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "" {
			continue
		}
		if _, exists := seen[field]; exists {
			continue
		}
		seen[field] = struct{}{}
		result = append(result, field)
	}
	sort.Strings(result)
	if len(result) == 0 {
		return nil
	}
	return result
}

func normalizeRules(rules map[string]models.ParserFieldRule) map[string]models.ParserFieldRule {
	if len(rules) == 0 {
		return map[string]models.ParserFieldRule{}
	}
	normalized := make(map[string]models.ParserFieldRule, len(rules))
	for target, rule := range rules {
		if target == "" {
			continue
		}
		normalized[target] = models.ParserFieldRule{
			Source:   rule.Source,
			Constant: rule.Constant,
		}
	}
	return normalized
}
