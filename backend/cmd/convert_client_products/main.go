package main

import (
	"context"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type sourceFile struct {
	Domains []sourceDomain `json:"domains"`
}

type sourceDomain struct {
	Domain string       `json:"domain"`
	Items  []sourceItem `json:"items"`
}

type sourceItem struct {
	Domain       string   `json:"domain"`
	URL          string   `json:"url"`
	Name         string   `json:"name"`
	GTIN         string   `json:"gtin"`
	Barcode      string   `json:"barcode"`
	Code         string   `json:"code"`
	CodeType     string   `json:"code_type"`
	ISBN         string   `json:"isbn"`
	EAN          string   `json:"ean"`
	Description  string   `json:"description"`
	Price        *float64 `json:"price"`
	Cost         *float64 `json:"cost"`
	Currency     string   `json:"currency"`
	Image        string   `json:"image"`
	Brand        string   `json:"brand"`
	Category     string   `json:"category"`
	Availability string   `json:"availability"`
	Source       string   `json:"source"`
	RequiredOK   bool     `json:"required_ok"`
	Required     []string `json:"required_fields"`
	Missing      []string `json:"missing_required"`
	Error        string   `json:"error"`
}

type eksmoImportDoc struct {
	GUID        string         `json:"guid,omitempty"`
	GUIDNOM     string         `json:"guidNom,omitempty"`
	NomCode     string         `json:"nomcode,omitempty"`
	ISBN        string         `json:"isbn,omitempty"`
	Name        string         `json:"name"`
	Annotation  string         `json:"annotation,omitempty"`
	CoverURL    string         `json:"coverUrl,omitempty"`
	SubjectName string         `json:"subjectName,omitempty"`
	BrandName   string         `json:"brandName,omitempty"`
	InMain      bool           `json:"inMainProducts,omitempty"`
	Raw         map[string]any `json:"raw,omitempty"`
	SyncedAt    time.Time      `json:"syncedAt"`
	UpdatedAt   time.Time      `json:"updatedAt"`
}

type mainImportRow struct {
	Name          string
	ISBN          string
	Annotation    string
	CoverURL      string
	SubjectName   string
	NicheName     string
	BrandName     string
	CategoryPath  string
	Price         string
	SourceGUIDNOM string
	SourceNomCode string
}

type candidate struct {
	Doc       eksmoImportDoc
	Main      mainImportRow
	Model     models.EksmoProduct
	Score     int
	Domain    string
	SourceURL string
}

type stats struct {
	InputItems            int
	SkippedNoName         int
	SkippedNoIdentifier   int
	SyntheticIdentifiers  int
	DeduplicatedReplaced  int
	DeduplicatedDiscarded int
	OutputItems           int
	WithPrice             int
	WithISBN              int
}

type normalizedIdentifiers struct {
	GTIN    string
	Barcode string
	Code    string
	ISBN    string
	EAN     string
}

var mainCSVHeader = []string{
	"id",
	"name",
	"isbn",
	"author_cover",
	"author_names",
	"annotation",
	"cover_url",
	"age_restriction",
	"subject_name",
	"niche_name",
	"brand_name",
	"series_name",
	"publisher_name",
	"quantity",
	"price",
	"category_id",
	"category_path",
	"source_guid_nom",
	"source_guid",
	"source_nomcode",
	"source_product_id",
}

func main() {
	inputPath := flag.String("input", "../products_client_all.json", "path to products_client_all.json")
	inputs := flag.String("inputs", "", "comma-separated file paths and/or glob patterns (for example: \"products_*.json\")")
	outEksmo := flag.String("out-eksmo", "../converted_eksmo_products.json", "path to write normalized eksmo products JSON")
	outMain := flag.String("out-main-csv", "../converted_main_products.csv", "path to write main products import CSV")
	skipEksmoJSON := flag.Bool("skip-eksmo-json", false, "skip writing normalized eksmo JSON output")
	skipMainCSV := flag.Bool("skip-main-csv", false, "skip writing main products CSV output")
	writeMongo := flag.Bool("write-mongo", false, "upsert normalized docs directly into eksmo_products collection")
	mongoURI := flag.String("mongo-uri", envOrDefault("MONGODB_URI", "mongodb://localhost:27017"), "MongoDB URI")
	mongoDB := flag.String("mongo-db", envOrDefault("MONGODB_DATABASE", "topar_db"), "MongoDB database")
	keepEmptyName := flag.Bool("keep-empty-name", false, "keep records without name by generating placeholder name")
	flag.Parse()

	src, loadedFiles, err := loadSourceInputs(*inputPath, *inputs)
	if err != nil {
		exitf("failed to load input: %v", err)
	}
	if *skipEksmoJSON && *skipMainCSV && !*writeMongo {
		exitf("nothing to do: enable at least one of write-mongo, eksmo json output, or main csv output")
	}
	fmt.Printf("loaded_input_files: %d\n", len(loadedFiles))
	for _, filePath := range loadedFiles {
		fmt.Printf("input_file: %s\n", filePath)
	}

	now := time.Now().UTC()
	records, st := convert(src, now, *keepEmptyName)
	if len(records) == 0 {
		exitf("no records produced after normalization")
	}

	eksmoDocs := make([]eksmoImportDoc, 0, len(records))
	mainRows := make([]mainImportRow, 0, len(records))
	modelsForUpsert := make([]models.EksmoProduct, 0, len(records))

	for _, rec := range records {
		eksmoDocs = append(eksmoDocs, rec.Doc)
		mainRows = append(mainRows, rec.Main)
		modelsForUpsert = append(modelsForUpsert, rec.Model)
	}

	if !*skipEksmoJSON {
		if err := writeEksmoJSON(*outEksmo, eksmoDocs); err != nil {
			exitf("failed writing eksmo output: %v", err)
		}
	}
	if !*skipMainCSV {
		if err := writeMainCSV(*outMain, mainRows); err != nil {
			exitf("failed writing main csv output: %v", err)
		}
	}

	fmt.Printf("conversion completed\n")
	fmt.Printf("input: %d\n", st.InputItems)
	fmt.Printf("output: %d\n", st.OutputItems)
	fmt.Printf("skipped_no_name: %d\n", st.SkippedNoName)
	fmt.Printf("skipped_no_identifier: %d\n", st.SkippedNoIdentifier)
	fmt.Printf("synthetic_identifiers: %d\n", st.SyntheticIdentifiers)
	fmt.Printf("dedup_replaced: %d\n", st.DeduplicatedReplaced)
	fmt.Printf("dedup_discarded: %d\n", st.DeduplicatedDiscarded)
	fmt.Printf("with_price: %d\n", st.WithPrice)
	fmt.Printf("with_isbn: %d\n", st.WithISBN)
	if !*skipEksmoJSON {
		fmt.Printf("eksmo_json: %s\n", *outEksmo)
	}
	if !*skipMainCSV {
		fmt.Printf("main_csv: %s\n", *outMain)
	}

	if *writeMongo {
		upserted, modified, skipped, err := upsertToMongo(*mongoURI, *mongoDB, modelsForUpsert)
		if err != nil {
			exitf("mongo upsert failed: %v", err)
		}
		fmt.Printf("mongo_upserted: %d\n", upserted)
		fmt.Printf("mongo_modified: %d\n", modified)
		fmt.Printf("mongo_skipped: %d\n", skipped)
	}
}

func convert(src sourceFile, now time.Time, keepEmptyName bool) ([]candidate, stats) {
	out := make(map[string]candidate)
	var st stats

	for _, bucket := range src.Domains {
		bucketDomain := normalizeField(bucket.Domain)
		for idx, item := range bucket.Items {
			st.InputItems++
			rowDomain := firstNonEmpty(normalizeField(item.Domain), bucketDomain)
			if rowDomain == "" {
				rowDomain = "unknown-domain"
			}

			name := normalizeField(item.Name)
			if name == "" && !keepEmptyName {
				st.SkippedNoName++
				continue
			}
			if name == "" {
				name = "Unnamed product " + strconv.Itoa(idx+1)
			}

			idFields := normalizeSourceIdentifiers(item)
			identifier, synthetic := buildIdentifier(rowDomain, item, idFields, idx)
			if identifier == "" {
				st.SkippedNoIdentifier++
				continue
			}
			if synthetic {
				st.SyntheticIdentifiers++
			}

			nomCode := identifier
			guidNom := "client:" + rowDomain + ":" + identifier
			guid := "client_url:" + shortHash(firstNonEmpty(item.URL, rowDomain+"|"+identifier))
			isbn := chooseISBN(item, idFields)
			price, hasPrice := choosePrice(item)
			image := extractPrimaryLink(normalizeField(item.Image))
			annotation := normalizeField(item.Description)
			categoryPathParts := splitCategoryPath(normalizeField(item.Category))
			subjectName := ""
			nicheName := ""
			categoryPath := ""
			if len(categoryPathParts) > 0 {
				subjectName = categoryPathParts[0]
				categoryPath = strings.Join(categoryPathParts, " / ")
			}
			if len(categoryPathParts) > 1 {
				nicheName = categoryPathParts[1]
			}

			rawMap := map[string]any{
				"SOURCE_KIND":   "products_client_all",
				"SOURCE_DOMAIN": rowDomain,
				"SOURCE_URL":    normalizeField(item.URL),
				"SOURCE_LABEL":  normalizeField(item.Source),
				"CODE_TYPE":     normalizeField(item.CodeType),
				"CATEGORY":      normalizeField(item.Category),
				"AVAILABILITY":  normalizeField(item.Availability),
				"CURRENCY":      normalizeField(item.Currency),
				"REQUIRED_OK":   item.RequiredOK,
				"REQUIRED":      item.Required,
			}
			if len(item.Missing) > 0 {
				rawMap["MISSING_REQUIRED"] = item.Missing
			}
			if errMsg := normalizeField(item.Error); errMsg != "" {
				rawMap["ERROR"] = errMsg
			}
			addRawIdentifierFields(rawMap, item.CodeType, idFields)
			addRawPriceFields(rawMap, item)

			doc := eksmoImportDoc{
				GUID:        guid,
				GUIDNOM:     guidNom,
				NomCode:     nomCode,
				ISBN:        isbn,
				Name:        name,
				Annotation:  annotation,
				CoverURL:    image,
				SubjectName: subjectName,
				BrandName:   normalizeField(item.Brand),
				Raw:         rawMap,
				SyncedAt:    now,
				UpdatedAt:   now,
			}

			model := models.EksmoProduct{
				GUID:           doc.GUID,
				GUIDNOM:        doc.GUIDNOM,
				NomCode:        doc.NomCode,
				ISBN:           doc.ISBN,
				Name:           doc.Name,
				Annotation:     doc.Annotation,
				CoverURL:       doc.CoverURL,
				SubjectName:    doc.SubjectName,
				BrandName:      doc.BrandName,
				Raw:            bson.M(rawMap),
				SyncedAt:       doc.SyncedAt,
				UpdatedAt:      doc.UpdatedAt,
				InMainProducts: false,
			}

			mainRow := mainImportRow{
				Name:          name,
				ISBN:          isbn,
				Annotation:    annotation,
				CoverURL:      image,
				SubjectName:   subjectName,
				NicheName:     nicheName,
				BrandName:     normalizeField(item.Brand),
				CategoryPath:  categoryPath,
				Price:         formatPrice(price, hasPrice),
				SourceGUIDNOM: guidNom,
				SourceNomCode: nomCode,
			}

			score := qualityScore(name, annotation, image, hasPrice, doc.BrandName, isbn, item.RequiredOK)
			current := candidate{
				Doc:       doc,
				Main:      mainRow,
				Model:     model,
				Score:     score,
				Domain:    rowDomain,
				SourceURL: normalizeField(item.URL),
			}

			existing, ok := out[guidNom]
			if !ok {
				out[guidNom] = current
				continue
			}

			if current.Score > existing.Score {
				out[guidNom] = current
				st.DeduplicatedReplaced++
			} else {
				st.DeduplicatedDiscarded++
			}
		}
	}

	result := make([]candidate, 0, len(out))
	for _, rec := range out {
		if rec.Main.Price != "" {
			st.WithPrice++
		}
		if rec.Doc.ISBN != "" {
			st.WithISBN++
		}
		result = append(result, rec)
	}
	st.OutputItems = len(result)
	return result, st
}

func writeEksmoJSON(path string, docs []eksmoImportDoc) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	content, err := json.MarshalIndent(docs, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	return os.WriteFile(path, content, 0o644)
}

func writeMainCSV(path string, rows []mainImportRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write(mainCSVHeader); err != nil {
		return err
	}
	for _, row := range rows {
		record := []string{
			"",
			row.Name,
			row.ISBN,
			"",
			"",
			row.Annotation,
			row.CoverURL,
			"",
			row.SubjectName,
			row.NicheName,
			row.BrandName,
			"",
			"",
			"",
			row.Price,
			"",
			row.CategoryPath,
			row.SourceGUIDNOM,
			"",
			row.SourceNomCode,
			"",
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func upsertToMongo(uri, dbName string, products []models.EksmoProduct) (int, int, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() {
		_ = client.Disconnect(context.Background())
	}()

	repo := repository.NewEksmoProductRepository(client.Database(dbName))
	if err := repo.EnsureIndexes(ctx); err != nil {
		return 0, 0, 0, err
	}
	return repo.UpsertBatch(ctx, products)
}

func loadSourceInputs(singleInput, multiInput string) (sourceFile, []string, error) {
	files, err := resolveInputFiles(singleInput, multiInput)
	if err != nil {
		return sourceFile{}, nil, err
	}

	merged := sourceFile{}
	for _, path := range files {
		raw, err := os.ReadFile(path)
		if err != nil {
			return sourceFile{}, nil, fmt.Errorf("failed to read %s: %w", path, err)
		}

		var src sourceFile
		if err := json.Unmarshal(raw, &src); err != nil {
			return sourceFile{}, nil, fmt.Errorf("failed to parse %s: %w", path, err)
		}

		merged.Domains = append(merged.Domains, src.Domains...)
	}

	return merged, files, nil
}

func resolveInputFiles(singleInput, multiInput string) ([]string, error) {
	candidates := make([]string, 0)
	if strings.TrimSpace(multiInput) != "" {
		for _, part := range strings.Split(multiInput, ",") {
			part = strings.TrimSpace(part)
			if part != "" {
				candidates = append(candidates, part)
			}
		}
	} else {
		candidates = append(candidates, strings.TrimSpace(singleInput))
	}

	found := make(map[string]struct{})
	paths := make([]string, 0)
	for _, candidate := range candidates {
		matches, err := filepath.Glob(candidate)
		if err != nil {
			return nil, fmt.Errorf("invalid input pattern %q: %w", candidate, err)
		}

		if len(matches) == 0 {
			if _, err := os.Stat(candidate); err == nil {
				matches = []string{candidate}
			}
		}

		if len(matches) == 0 {
			return nil, fmt.Errorf("no files matched input %q", candidate)
		}

		for _, match := range matches {
			if _, exists := found[match]; exists {
				continue
			}
			found[match] = struct{}{}
			paths = append(paths, match)
		}
	}

	sort.Strings(paths)
	return paths, nil
}

func buildIdentifier(domain string, item sourceItem, ids normalizedIdentifiers, rowIndex int) (string, bool) {
	for _, normalized := range dedupeValues(ids.Code, ids.Barcode, ids.GTIN, ids.EAN, ids.ISBN) {
		if normalized != "" {
			return normalized, false
		}
	}

	url := normalizeField(item.URL)
	if url != "" {
		return "url-" + shortHash(url), true
	}
	name := normalizeField(item.Name)
	if name != "" {
		return "name-" + shortHash(domain+"|"+name), true
	}
	return "row-" + strconv.Itoa(rowIndex+1), true
}

func normalizeSourceIdentifiers(item sourceItem) normalizedIdentifiers {
	return normalizedIdentifiers{
		GTIN:    normalizeIdentifier(item.GTIN),
		Barcode: normalizeIdentifier(item.Barcode),
		Code:    normalizeIdentifier(item.Code),
		ISBN:    normalizeISBN(item.ISBN),
		EAN:     normalizeISBN(item.EAN),
	}
}

func addRawIdentifierFields(raw map[string]any, codeType string, ids normalizedIdentifiers) {
	type pair struct {
		key   string
		value string
	}
	pairs := []pair{
		{key: "ISBN", value: ids.ISBN},
		{key: "EAN", value: ids.EAN},
		{key: "GTIN", value: ids.GTIN},
		{key: "BARCODE", value: ids.Barcode},
		{key: "CODE", value: ids.Code},
	}

	normalizedCodeType := strings.ToLower(normalizeField(codeType))
	if normalizedCodeType == "isbn" && ids.Code != "" {
		pairs = append([]pair{{key: "ISBN", value: normalizeISBN(ids.Code)}}, pairs...)
	}
	if normalizedCodeType == "ean" && ids.Code != "" {
		pairs = append([]pair{{key: "EAN", value: normalizeISBN(ids.Code)}}, pairs...)
	}

	seen := make(map[string]struct{}, len(pairs))
	for _, pair := range pairs {
		if pair.value == "" {
			continue
		}
		dedupKey := strings.ToUpper(strings.TrimSpace(pair.value))
		if dedupKey == "" {
			continue
		}
		if _, exists := seen[dedupKey]; exists {
			continue
		}
		seen[dedupKey] = struct{}{}
		raw[pair.key] = pair.value
	}
}

func addRawPriceFields(raw map[string]any, item sourceItem) {
	price, hasPrice := positivePrice(item.Price)
	cost, hasCost := positivePrice(item.Cost)

	if hasPrice {
		raw["PRICE"] = price
	}
	if hasCost && (!hasPrice || !sameFloat(price, cost)) {
		raw["COST"] = cost
	}
}

func normalizeIdentifier(value string) string {
	value = normalizeField(value)
	value = strings.ReplaceAll(value, " ", "")
	value = strings.ReplaceAll(value, "\u00a0", "")
	return value
}

func chooseISBN(item sourceItem, ids normalizedIdentifiers) string {
	candidates := []string{
		ids.ISBN,
		ids.EAN,
	}
	if strings.EqualFold(normalizeField(item.CodeType), "isbn") || strings.EqualFold(normalizeField(item.CodeType), "ean") {
		candidates = append(candidates, normalizeISBN(ids.Code))
	}
	candidates = append(candidates, normalizeISBN(ids.Barcode), normalizeISBN(ids.Code), normalizeISBN(ids.GTIN))
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if looksLikeISBNOrEAN(candidate) {
			return candidate
		}
	}
	return ""
}

func choosePrice(item sourceItem) (float64, bool) {
	if value, ok := positivePrice(item.Cost); ok {
		return value, true
	}
	if value, ok := positivePrice(item.Price); ok {
		return value, true
	}
	return 0, false
}

func positivePrice(value *float64) (float64, bool) {
	if value == nil || *value <= 0 {
		return 0, false
	}
	return *value, true
}

func sameFloat(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 1e-9
}

func dedupeValues(values ...string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		dedupKey := strings.ToUpper(trimmed)
		if _, exists := seen[dedupKey]; exists {
			continue
		}
		seen[dedupKey] = struct{}{}
		out = append(out, trimmed)
	}
	return out
}

func splitCategoryPath(category string) []string {
	category = normalizeField(category)
	if category == "" {
		return nil
	}
	separators := []string{" / ", ">", "»", "->", "|", "/"}
	parts := []string{category}
	for _, sep := range separators {
		if strings.Contains(category, sep) {
			parts = strings.Split(category, sep)
			break
		}
	}

	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = normalizeField(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func extractPrimaryLink(value string) string {
	value = normalizeField(value)
	if value == "" {
		return ""
	}
	separators := []string{";", ",", "|", " "}
	for _, sep := range separators {
		if !strings.Contains(value, sep) {
			continue
		}
		parts := strings.Split(value, sep)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			lower := strings.ToLower(part)
			if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
				return part
			}
		}
	}
	return value
}

func normalizeISBN(value string) string {
	trimmed := normalizeField(value)
	if trimmed == "" {
		return ""
	}
	trimmed = strings.ReplaceAll(trimmed, "-", "")
	trimmed = strings.ReplaceAll(trimmed, " ", "")
	return trimmed
}

func looksLikeISBNOrEAN(value string) bool {
	if value == "" {
		return false
	}
	cleaned := strings.ToUpper(strings.TrimSpace(value))
	if len(cleaned) != 8 && len(cleaned) != 10 && len(cleaned) != 13 && len(cleaned) != 14 {
		return false
	}
	for i, r := range cleaned {
		if r >= '0' && r <= '9' {
			continue
		}
		if len(cleaned) == 10 && i == 9 && r == 'X' {
			continue
		}
		return false
	}
	return true
}

func qualityScore(name, annotation, coverURL string, hasPrice bool, brand, isbn string, requiredOK bool) int {
	score := 0
	if name != "" {
		score += 100
	}
	if annotation != "" {
		score += 15
	}
	if coverURL != "" {
		score += 20
	}
	if hasPrice {
		score += 20
	}
	if brand != "" {
		score += 10
	}
	if isbn != "" {
		score += 10
	}
	if requiredOK {
		score += 5
	}
	return score
}

func formatPrice(price float64, ok bool) string {
	if !ok || price <= 0 {
		return ""
	}
	return strconv.FormatFloat(price, 'f', -1, 64)
}

func normalizeField(value string) string {
	return strings.TrimSpace(strings.ReplaceAll(value, "\u00a0", " "))
}

func shortHash(value string) string {
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:8])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func envOrDefault(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
