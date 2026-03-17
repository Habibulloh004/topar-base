package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"
	"topar/backend/internal/services"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type ParserAppHandler struct {
	repo    *repository.ParserAppRepository
	service *services.ParserAppService
	redis   *redis.Client
}

func NewParserAppHandler(
	repo *repository.ParserAppRepository,
	service *services.ParserAppService,
	redisClient *redis.Client,
) *ParserAppHandler {
	return &ParserAppHandler{
		repo:    repo,
		service: service,
		redis:   redisClient,
	}
}

func (h *ParserAppHandler) RegisterRoutes(app *fiber.App) {
	app.Get("/parser-app/schema", h.GetSchema)
	app.Get("/parser-app/runs", h.ListRuns)
	app.Get("/parser-app/runs/:id", h.GetRun)
	app.Get("/parser-app/runs/:id/records", h.GetRunRecords)
	app.Post("/parser-app/parse", h.Parse)
	app.Get("/parser-app/mappings", h.ListMappings)
	app.Post("/parser-app/mappings", h.SaveMapping)
	app.Post("/parser-app/sync-local", h.SyncLocalRecords)
	app.Post("/parser-app/runs/:id/sync", h.SyncRun)
	app.Post("/parser-app/runs/:id/seed", h.SeedRun)
}

func (h *ParserAppHandler) GetSchema(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"target": h.service.GetTargetSchema(),
	})
}

func (h *ParserAppHandler) ListRuns(c *fiber.Ctx) error {
	limit := int64(parseIntQuery(c, "limit", 20))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	runs, err := h.repo.ListRuns(ctx, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(fiber.Map{"items": runs})
}

func (h *ParserAppHandler) GetRun(c *fiber.Ctx) error {
	runID, err := parseObjectIDParam(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	run, exists, err := h.repo.GetRun(ctx, runID)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if !exists {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "run not found"})
	}

	limit := int64(parseIntQuery(c, "limit", 20))
	records, total, err := h.repo.ListRunRecords(ctx, runID, 1, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	if len(run.DetectedFields) == 0 {
		fields, detectErr := h.repo.DetectRunFields(ctx, runID, 500)
		if detectErr == nil {
			run.DetectedFields = fields
		}
	}

	return c.JSON(fiber.Map{
		"run":     run,
		"total":   total,
		"records": records,
	})
}

func (h *ParserAppHandler) GetRunRecords(c *fiber.Ctx) error {
	runID, err := parseObjectIDParam(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	records, total, err := h.repo.ListRunRecords(ctx, runID, page, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"items": records,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

func (h *ParserAppHandler) Parse(c *fiber.Ctx) error {
	var req services.ParserParseRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()

	result, err := h.service.ParseAndStore(ctx, req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"run":    result.Run,
		"sample": result.Sample,
	})
}

func (h *ParserAppHandler) ListMappings(c *fiber.Ctx) error {
	limit := int64(parseIntQuery(c, "limit", 20))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	profiles, err := h.repo.ListMappingProfiles(ctx, limit)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{"items": profiles})
}

func (h *ParserAppHandler) SaveMapping(c *fiber.Ctx) error {
	var payload struct {
		Name  string                            `json:"name"`
		Rules map[string]models.ParserFieldRule `json:"rules"`
	}
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if len(payload.Rules) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "rules are required"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	profile, err := h.repo.SaveMappingProfile(ctx, strings.TrimSpace(payload.Name), payload.Rules)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(profile)
}

func (h *ParserAppHandler) SyncRun(c *fiber.Ctx) error {
	return h.sync(c, false)
}

func (h *ParserAppHandler) SeedRun(c *fiber.Ctx) error {
	return h.sync(c, true)
}

func (h *ParserAppHandler) SyncLocalRecords(c *fiber.Ctx) error {
	req, err := parseLocalSyncRequest(c.Body())
	if err != nil {
		log.Printf("parser-app sync-local invalid request body: err=%v bytes=%d", err, len(c.Body()))
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	result, err := h.service.SyncLocalRecords(ctx, req)
	if err != nil {
		log.Printf(
			"parser-app sync-local failed: err=%v runId=%q records=%d invalid=%d rules=%d bytes=%d",
			err,
			req.RunID,
			len(req.Records),
			len(req.Invalid),
			len(req.Rules),
			len(c.Body()),
		)
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	invalidateProductCachesByRedis(h.redis)
	return c.JSON(result)
}

func (h *ParserAppHandler) sync(c *fiber.Ctx, seedingMode bool) error {
	runID, err := parseObjectIDParam(c.Params("id"))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}

	var req services.ParserSyncRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}
	if seedingMode {
		req.SyncEksmo = true
		req.SyncMain = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

	result, err := h.service.SyncRun(ctx, runID, req)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	invalidateProductCachesByRedis(h.redis)
	return c.JSON(result)
}

func parseObjectIDParam(value string) (primitive.ObjectID, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return primitive.NilObjectID, fiber.NewError(fiber.StatusBadRequest, "id is required")
	}
	id, err := primitive.ObjectIDFromHex(trimmed)
	if err != nil {
		return primitive.NilObjectID, fiber.NewError(fiber.StatusBadRequest, "id is invalid")
	}
	return id, nil
}

func parseLocalSyncRequest(body []byte) (services.ParserLocalSyncRequest, error) {
	req := services.ParserLocalSyncRequest{}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()

	var payload map[string]json.RawMessage
	if err := decoder.Decode(&payload); err != nil {
		recoveredReq, recoveredErr := parseLocalSyncRequestBestEffort(body)
		if recoveredErr != nil {
			return req, err
		}
		return recoveredReq, nil
	}

	return parseLocalSyncRequestFromPayload(payload)
}

func parseLocalSyncRequestBestEffort(body []byte) (services.ParserLocalSyncRequest, error) {
	payload := map[string]json.RawMessage{}
	keys := []string{
		"runId",
		"records",
		"rules",
		"saveMapping",
		"mappingName",
		"syncEksmo",
		"syncMain",
	}

	for _, key := range keys {
		raw := extractJSONFieldRaw(body, key)
		if len(raw) == 0 {
			continue
		}
		payload[key] = raw
	}
	if len(payload) == 0 {
		return services.ParserLocalSyncRequest{}, fiber.ErrBadRequest
	}
	return parseLocalSyncRequestFromPayload(payload)
}

func parseLocalSyncRequestFromPayload(payload map[string]json.RawMessage) (services.ParserLocalSyncRequest, error) {
	req := services.ParserLocalSyncRequest{}
	req.RunID = stringifyRawJSONValue(payload["runId"])
	req.Rules = parseLocalSyncRules(payload["rules"])
	req.SaveMapping = parseRawJSONBool(payload["saveMapping"])
	req.MappingName = stringifyRawJSONValue(payload["mappingName"])
	req.SyncEksmo = parseRawJSONBool(payload["syncEksmo"])
	req.SyncMain = parseRawJSONBool(payload["syncMain"])

	recordBodies, invalidRecords, err := parseLocalSyncRecordBodies(payload["records"])
	if err != nil {
		return req, err
	}

	req.Records = make([]services.ParserLocalSyncRecord, 0, len(recordBodies))
	req.Invalid = make([]services.ParserInvalidRecord, 0, len(invalidRecords))
	req.Invalid = append(req.Invalid, invalidRecords...)

	for _, recordBody := range recordBodies {
		record, invalid := parseLocalSyncRecord(recordBody)
		if invalid != nil {
			req.Invalid = append(req.Invalid, *invalid)
			continue
		}
		req.Records = append(req.Records, *record)
	}

	return req, nil
}

func parseLocalSyncRecordBodies(raw json.RawMessage) ([]json.RawMessage, []services.ParserInvalidRecord, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil, nil
	}

	var records []json.RawMessage
	if err := json.Unmarshal(raw, &records); err != nil {
		recoveredRecords, brokenSegments, recoverErr := recoverLocalSyncRecordBodies(raw)
		if recoverErr != nil {
			return nil, nil, fiber.ErrBadRequest
		}
		invalid := make([]services.ParserInvalidRecord, 0, len(brokenSegments))
		for _, segment := range brokenSegments {
			invalid = append(invalid, services.ParserInvalidRecord{
				Error:   "record is not valid json: malformed segment in records array",
				Payload: map[string]any{"raw": string(segment)},
			})
		}
		if len(recoveredRecords) == 0 && len(invalid) == 0 {
			return nil, nil, fiber.ErrBadRequest
		}
		return recoveredRecords, invalid, nil
	}
	return records, nil, nil
}

func parseLocalSyncRules(raw json.RawMessage) map[string]models.ParserFieldRule {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}

	var rawRules map[string]json.RawMessage
	if err := json.Unmarshal(raw, &rawRules); err != nil {
		return nil
	}

	rules := make(map[string]models.ParserFieldRule, len(rawRules))
	for key, ruleBody := range rawRules {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}

		var rulePayload map[string]json.RawMessage
		if err := json.Unmarshal(ruleBody, &rulePayload); err != nil {
			continue
		}

		rule := models.ParserFieldRule{
			Source:   stringifyRawJSONValue(rulePayload["source"]),
			Constant: stringifyRawJSONValue(rulePayload["constant"]),
		}
		if rule.Source == "" && rule.Constant == "" {
			continue
		}
		rules[key] = rule
	}

	if len(rules) == 0 {
		return nil
	}
	return rules
}

func parseLocalSyncRecord(recordBody json.RawMessage) (*services.ParserLocalSyncRecord, *services.ParserInvalidRecord) {
	var payload any
	if err := json.Unmarshal(recordBody, &payload); err != nil {
		return nil, &services.ParserInvalidRecord{
			Error:   fmt.Sprintf("record is not valid json: %v", err),
			Payload: map[string]any{"raw": string(recordBody)},
		}
	}

	recordMap, ok := payload.(map[string]any)
	if !ok {
		return nil, &services.ParserInvalidRecord{
			Error:   "record must be an object",
			Payload: payload,
		}
	}

	dataValue, hasData := recordMap["data"]
	if !hasData {
		return nil, &services.ParserInvalidRecord{
			SourceURL: stringifyJSONValue(recordMap["sourceUrl"]),
			Error:     "record.data is required",
			Payload:   recordMap,
		}
	}

	dataMap, ok := dataValue.(map[string]any)
	if !ok {
		return nil, &services.ParserInvalidRecord{
			SourceURL: stringifyJSONValue(recordMap["sourceUrl"]),
			Error:     "record.data must be an object",
			Payload:   recordMap,
		}
	}

	return &services.ParserLocalSyncRecord{
		SourceURL: stringifyJSONValue(recordMap["sourceUrl"]),
		Data:      dataMap,
	}, nil
}

func stringifyJSONValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return strings.TrimSpace(typed.String())
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case []any, map[string]any:
		return ""
	default:
		return strings.TrimSpace(fmt.Sprint(typed))
	}
}

func stringifyRawJSONValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return ""
	}
	return stringifyJSONValue(value)
}

func parseRawJSONBool(raw json.RawMessage) bool {
	if len(raw) == 0 {
		return false
	}

	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return false
	}

	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func extractJSONFieldRaw(body []byte, field string) json.RawMessage {
	needle := []byte(`"` + field + `"`)
	searchFrom := 0
	for searchFrom < len(body) {
		index := bytes.Index(body[searchFrom:], needle)
		if index < 0 {
			return nil
		}
		index += searchFrom

		colon := skipJSONWhitespace(body, index+len(needle))
		if colon >= len(body) || body[colon] != ':' {
			searchFrom = index + len(needle)
			continue
		}

		valueStart := skipJSONWhitespace(body, colon+1)
		valueEnd, _, ok := scanJSONValueEnd(body, valueStart)
		if !ok || valueEnd <= valueStart {
			searchFrom = index + len(needle)
			continue
		}

		return cloneRawJSON(bytes.TrimSpace(body[valueStart:valueEnd]))
	}
	return nil
}

func recoverLocalSyncRecordBodies(raw json.RawMessage) ([]json.RawMessage, []json.RawMessage, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil
	}
	if trimmed[0] != '[' {
		return nil, nil, fiber.ErrBadRequest
	}

	records := make([]json.RawMessage, 0)
	broken := make([]json.RawMessage, 0)

	inString := false
	escapeNext := false
	objectDepth := 0
	elementStart := -1

	for i := 1; i < len(trimmed); i++ {
		ch := trimmed[i]

		if inString {
			if escapeNext {
				escapeNext = false
				continue
			}
			if ch == '\\' {
				escapeNext = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}

		if ch == '"' {
			inString = true
			if elementStart == -1 {
				elementStart = i
			}
			continue
		}

		if objectDepth > 0 {
			if ch == '{' {
				objectDepth++
				continue
			}
			if ch == '}' {
				objectDepth--
				if objectDepth == 0 && elementStart >= 0 {
					candidate := bytes.TrimSpace(trimmed[elementStart : i+1])
					if len(candidate) > 0 {
						records = append(records, cloneRawJSON(candidate))
					}
					elementStart = -1
				}
			}
			continue
		}

		switch ch {
		case '{':
			if elementStart >= 0 {
				segment := bytes.TrimSpace(trimmed[elementStart:i])
				if len(segment) > 0 {
					broken = append(broken, cloneRawJSON(segment))
				}
			}
			elementStart = i
			objectDepth = 1
		case ',':
			if elementStart >= 0 {
				segment := bytes.TrimSpace(trimmed[elementStart:i])
				if len(segment) > 0 {
					broken = append(broken, cloneRawJSON(segment))
				}
				elementStart = -1
			}
		case ']':
			if elementStart >= 0 {
				segment := bytes.TrimSpace(trimmed[elementStart:i])
				if len(segment) > 0 {
					broken = append(broken, cloneRawJSON(segment))
				}
			}
			return records, broken, nil
		default:
			if isJSONWhitespace(ch) {
				continue
			}
			if elementStart == -1 {
				elementStart = i
			}
		}
	}

	if elementStart >= 0 {
		segment := bytes.TrimSpace(trimmed[elementStart:])
		if len(segment) > 0 {
			broken = append(broken, cloneRawJSON(segment))
		}
	}

	return records, broken, nil
}

func skipJSONWhitespace(data []byte, index int) int {
	for index < len(data) && isJSONWhitespace(data[index]) {
		index++
	}
	return index
}

func scanJSONValueEnd(data []byte, start int) (int, byte, bool) {
	if start >= len(data) {
		return start, 0, false
	}

	inString := false
	escapeNext := false
	objectDepth := 0
	arrayDepth := 0
	valueStarted := false

	for i := start; i < len(data); i++ {
		ch := data[i]

		if inString {
			if escapeNext {
				escapeNext = false
				continue
			}
			if ch == '\\' {
				escapeNext = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}

		if ch == '"' {
			inString = true
			valueStarted = true
			continue
		}
		if isJSONWhitespace(ch) {
			continue
		}

		switch ch {
		case '{':
			objectDepth++
			valueStarted = true
		case '[':
			arrayDepth++
			valueStarted = true
		case '}':
			if objectDepth == 0 && arrayDepth == 0 && valueStarted {
				return i, ch, true
			}
			if objectDepth > 0 {
				objectDepth--
			}
			valueStarted = true
		case ']':
			if arrayDepth == 0 && objectDepth == 0 && valueStarted {
				return i, ch, true
			}
			if arrayDepth > 0 {
				arrayDepth--
			}
			valueStarted = true
		case ',':
			if objectDepth == 0 && arrayDepth == 0 && valueStarted {
				return i, ch, true
			}
			valueStarted = true
		default:
			valueStarted = true
		}

		if valueStarted && objectDepth == 0 && arrayDepth == 0 {
			next := skipJSONWhitespace(data, i+1)
			if next >= len(data) {
				return i + 1, 0, true
			}
			if data[next] == ',' || data[next] == '}' || data[next] == ']' {
				return i + 1, data[next], true
			}
		}
	}

	if valueStarted {
		return len(data), 0, true
	}
	return start, 0, false
}

func isJSONWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'
}

func cloneRawJSON(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	copied := make([]byte, len(raw))
	copy(copied, raw)
	return copied
}
