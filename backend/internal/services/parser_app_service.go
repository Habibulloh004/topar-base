package services

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	stdhtml "html"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	xhtml "golang.org/x/net/html"
)

type ParserParseRequest struct {
	SourceURL      string  `json:"sourceUrl"`
	Limit          int     `json:"limit"`
	Workers        int     `json:"workers"`
	RequestsPerSec float64 `json:"requestsPerSec"`
	MaxSitemaps    int     `json:"maxSitemaps"`
}

type ParserSyncRequest struct {
	Rules       map[string]models.ParserFieldRule `json:"rules"`
	SaveMapping bool                              `json:"saveMapping"`
	MappingName string                            `json:"mappingName"`
	SyncEksmo   bool                              `json:"syncEksmo"`
	SyncMain    bool                              `json:"syncMain"`
}

type ParserLocalSyncRecord struct {
	SourceURL string         `json:"sourceUrl"`
	Data      map[string]any `json:"data"`
}

type ParserInvalidRecord struct {
	SourceURL string
	Payload   any
	Error     string
}

type ParserLocalSyncRequest struct {
	RunID       string                            `json:"runId"`
	Records     []ParserLocalSyncRecord           `json:"records"`
	Rules       map[string]models.ParserFieldRule `json:"rules"`
	SaveMapping bool                              `json:"saveMapping"`
	MappingName string                            `json:"mappingName"`
	SyncEksmo   bool                              `json:"syncEksmo"`
	SyncMain    bool                              `json:"syncMain"`
	Invalid     []ParserInvalidRecord             `json:"-"`
}

type ParserRunExecution struct {
	Run    models.ParserRun      `json:"run"`
	Sample []models.ParserRecord `json:"sample"`
}

const (
	parserMaxProducts        = 1_000_000
	parserMaxDiscoveredURLs  = 1_000_000
	parserDefaultMaxSitemaps = 500
	parserMaxSitemaps        = 100_000
	parserRecordFlushBatch   = 500
	parserSampleSize         = 20
)

type parserURLParseResult struct {
	ParsedCount      int
	RateLimitRetries int
	Sample           []models.ParserRecord
	DetectedFields   []string
}

type ParserAppService struct {
	parserRepo  *repository.ParserAppRepository
	eksmoRepo   *repository.EksmoProductRepository
	mainRepo    *repository.MainProductRepository
	invalidRepo *repository.InvalidProductRepository
	httpClient  *http.Client
	userAgent   string
}

func NewParserAppService(
	parserRepo *repository.ParserAppRepository,
	eksmoRepo *repository.EksmoProductRepository,
	mainRepo *repository.MainProductRepository,
	invalidRepo *repository.InvalidProductRepository,
) *ParserAppService {
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         dialer.DialContext,
		MaxIdleConns:        100,
		MaxConnsPerHost:     30,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	return &ParserAppService{
		parserRepo:  parserRepo,
		eksmoRepo:   eksmoRepo,
		mainRepo:    mainRepo,
		invalidRepo: invalidRepo,
		httpClient:  &http.Client{Transport: transport, Timeout: 20 * time.Second},
		userAgent:   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
	}
}

func (s *ParserAppService) ParseAndStore(ctx context.Context, req ParserParseRequest) (ParserRunExecution, error) {
	sourceURL, err := normalizeURL(req.SourceURL)
	if err != nil {
		return ParserRunExecution{}, err
	}

	limit := req.Limit
	if limit < 0 {
		limit = 0
	}
	// limit=0 means "full parse", but we still guard to 1M products.
	if limit > parserMaxProducts {
		limit = parserMaxProducts
	}

	workers := req.Workers
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}

	rps := req.RequestsPerSec
	if rps <= 0 {
		rps = 3
	}
	if rps < 1 {
		rps = 1
	}
	if rps > 20 {
		rps = 20
	}

	maxSitemaps := req.MaxSitemaps
	if maxSitemaps < 1 {
		maxSitemaps = parserDefaultMaxSitemaps
	}
	if maxSitemaps > parserMaxSitemaps {
		maxSitemaps = parserMaxSitemaps
	}

	run, err := s.parserRepo.CreateRun(ctx, models.ParserRun{
		SourceURL:      sourceURL,
		Limit:          limit,
		Workers:        workers,
		RequestsPerSec: rps,
		Status:         models.ParserRunStatusRunning,
	})
	if err != nil {
		return ParserRunExecution{}, err
	}
	if blockErr := s.detectSourceBlocking(ctx, sourceURL); blockErr != nil {
		_ = s.parserRepo.FinishRun(ctx, run.ID, nil, 0, 0, 0, blockErr.Error())
		return ParserRunExecution{}, blockErr
	}

	urls, discoverErr := s.discoverCandidateURLs(ctx, sourceURL, limit, maxSitemaps, rps)
	if discoverErr != nil {
		_ = s.parserRepo.FinishRun(ctx, run.ID, nil, 0, 0, 0, discoverErr.Error())
		return ParserRunExecution{}, discoverErr
	}
	if len(urls) == 0 {
		urls = []string{sourceURL}
	}

	parseResult, parseErr := s.parseURLs(ctx, run.ID, urls, workers, rps, limit)
	if parseErr != nil {
		_ = s.parserRepo.FinishRun(ctx, run.ID, parseResult.DetectedFields, len(urls), parseResult.ParsedCount, parseResult.RateLimitRetries, parseErr.Error())
		return ParserRunExecution{}, parseErr
	}
	if parseResult.ParsedCount == 0 {
		if blockErr := s.detectSourceBlocking(ctx, sourceURL); blockErr != nil {
			_ = s.parserRepo.FinishRun(ctx, run.ID, nil, len(urls), 0, parseResult.RateLimitRetries, blockErr.Error())
			return ParserRunExecution{}, blockErr
		}
	}
	if err := s.parserRepo.FinishRun(ctx, run.ID, parseResult.DetectedFields, len(urls), parseResult.ParsedCount, parseResult.RateLimitRetries, ""); err != nil {
		return ParserRunExecution{}, err
	}

	updatedRun, exists, err := s.parserRepo.GetRun(ctx, run.ID)
	if err != nil {
		return ParserRunExecution{}, err
	}
	if !exists {
		return ParserRunExecution{}, errors.New("parser run not found after save")
	}

	return ParserRunExecution{Run: updatedRun, Sample: parseResult.Sample}, nil
}

func (s *ParserAppService) GetTargetSchema() models.ParserTargetSchema {
	return models.ParserTargetSchema{
		Eksmo: []models.ParserSchemaField{
			{Key: "guidNom", Description: "Stable product id in eksmo_products"},
			{Key: "guid"},
			{Key: "nomcode", Description: "Barcode/nomcode field"},
			{Key: "barcode", Description: "Alias for nomcode"},
			{Key: "isbn"},
			{Key: "name"},
			{Key: "annotation"},
			{Key: "authorCover"},
			{Key: "authorNames"},
			{Key: "coverUrl"},
			{Key: "subjectName"},
			{Key: "nicheName"},
			{Key: "brandName"},
			{Key: "serieName"},
			{Key: "publisher"},
			{Key: "pages"},
			{Key: "format"},
			{Key: "paperType"},
			{Key: "bindingType"},
			{Key: "ageRestriction"},
			{Key: "price", Description: "Stored in raw.PRICE"},
		},
		Main: []models.ParserSchemaField{
			{Key: "sourceGuidNom"},
			{Key: "sourceGuid"},
			{Key: "sourceNomcode"},
			{Key: "barcode", Description: "Alias for sourceNomcode"},
			{Key: "isbn"},
			{Key: "name"},
			{Key: "annotation"},
			{Key: "authorCover"},
			{Key: "authorNames"},
			{Key: "coverUrl"},
			{Key: "subjectName"},
			{Key: "nicheName"},
			{Key: "brandName"},
			{Key: "seriesName"},
			{Key: "publisherName"},
			{Key: "ageRestriction"},
			{Key: "quantity"},
			{Key: "price"},
			{Key: "categoryPath", Description: "String like books>fiction>novel"},
			{Key: "categoryId", Description: "Mongo ObjectID"},
		},
	}
}

func (s *ParserAppService) SyncRun(
	ctx context.Context,
	runID primitive.ObjectID,
	req ParserSyncRequest,
) (models.ParserSyncResult, error) {
	result := models.ParserSyncResult{RunID: runID}
	if runID.IsZero() {
		return result, errors.New("run id is required")
	}
	if len(req.Rules) == 0 {
		return result, errors.New("at least one mapping rule is required")
	}

	req.SyncEksmo = true
	req.SyncMain = true

	if req.SaveMapping || strings.TrimSpace(req.MappingName) != "" {
		profile, err := s.parserRepo.SaveMappingProfile(ctx, strings.TrimSpace(req.MappingName), req.Rules)
		if err != nil {
			return result, err
		}
		result.MappingProfileID = profile.ID.Hex()
		result.MappingProfile = profile.Name
	}

	run, exists, err := s.parserRepo.GetRun(ctx, runID)
	if err != nil {
		return result, err
	}
	if !exists {
		return result, errors.New("run not found")
	}
	if run.Status != models.ParserRunStatusFinished {
		return result, errors.New("run is not finished")
	}

	cursor, err := s.parserRepo.StreamRunRecords(ctx, runID, 300)
	if err != nil {
		return result, err
	}
	defer cursor.Close(ctx)

	eksmoBatch := make([]models.EksmoProduct, 0, 300)
	mainBatch := make([]models.MainProduct, 0, 300)
	invalidProducts := make([]models.InvalidProduct, 0, 64)

	flush := func() error {
		if req.SyncEksmo && len(eksmoBatch) > 0 {
			upserted, modified, skipped, upsertErr := s.eksmoRepo.UpsertBatch(ctx, eksmoBatch)
			if upsertErr != nil {
				return upsertErr
			}
			result.EksmoUpserted += upserted
			result.EksmoModified += modified
			result.EksmoSkipped += skipped
			eksmoBatch = eksmoBatch[:0]
		}
		if req.SyncMain && len(mainBatch) > 0 {
			inserted, modified, skipped, upsertErr := s.mainRepo.UpsertImported(ctx, mainBatch)
			if upsertErr != nil {
				return upsertErr
			}
			result.MainInserted += inserted
			result.MainModified += modified
			result.MainSkipped += skipped
			mainBatch = mainBatch[:0]
		}
		return nil
	}

	for cursor.Next(ctx) {
		var record models.ParserRecord
		if err := cursor.Decode(&record); err != nil {
			continue
		}
		result.TotalRecords++

		eksmoMapped := applyRules(record.Data, req.Rules, "eksmo.")
		mainMapped := applyRules(record.Data, req.Rules, "main.")

		parsedEksmo, hasEksmo := buildEksmoProductFromMapping(eksmoMapped, record.Data, runID)
		if req.SyncEksmo {
			if !hasEksmo {
				result.EksmoSkipped++
			} else {
				eksmoBatch = append(eksmoBatch, parsedEksmo)
			}
		}

		if req.SyncMain {
			parsedMain, hasMain := buildMainProductFromMapping(mainMapped, record.Data, parsedEksmo)
			if !hasMain {
				result.MainSkipped++
			} else {
				mainBatch = append(mainBatch, parsedMain)
			}
			if !hasEksmo && !hasMain {
				invalidProducts = append(invalidProducts, newInvalidProduct(
					runID,
					record.SourceURL,
					"parser-run-sync",
					"record did not produce a valid eksmo or main product",
					record.Data,
				))
				result.InvalidCount++
			}
		} else if !hasEksmo {
			invalidProducts = append(invalidProducts, newInvalidProduct(
				runID,
				record.SourceURL,
				"parser-run-sync",
				"record did not produce a valid eksmo product",
				record.Data,
			))
			result.InvalidCount++
		}

		if len(eksmoBatch) >= 250 || len(mainBatch) >= 250 {
			if err := flush(); err != nil {
				return result, err
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return result, err
	}

	if err := flush(); err != nil {
		return result, err
	}
	if err := s.saveInvalidProducts(ctx, invalidProducts); err != nil {
		return result, err
	}

	return result, nil
}

func (s *ParserAppService) SyncLocalRecords(
	ctx context.Context,
	req ParserLocalSyncRequest,
) (models.ParserSyncResult, error) {
	result := models.ParserSyncResult{}

	if len(req.Records) == 0 && len(req.Invalid) == 0 {
		return result, errors.New("at least one record is required")
	}

	req.SyncEksmo = true
	req.SyncMain = true

	if len(req.Rules) > 0 && (req.SaveMapping || strings.TrimSpace(req.MappingName) != "") {
		profile, err := s.parserRepo.SaveMappingProfile(ctx, strings.TrimSpace(req.MappingName), req.Rules)
		if err != nil {
			return result, err
		}
		result.MappingProfileID = profile.ID.Hex()
		result.MappingProfile = profile.Name
	}

	runID := primitive.NilObjectID
	runIDCandidate := strings.TrimSpace(req.RunID)
	if runIDCandidate != "" {
		if parsed, err := primitive.ObjectIDFromHex(runIDCandidate); err == nil {
			runID = parsed
		} else {
			cleaned := strings.ToLower(strings.ReplaceAll(runIDCandidate, "-", ""))
			if len(cleaned) >= 24 {
				if parsed, parseErr := primitive.ObjectIDFromHex(cleaned[:24]); parseErr == nil {
					runID = parsed
				}
			}
		}
	}
	result.RunID = runID

	eksmoBatch := make([]models.EksmoProduct, 0, 300)
	mainBatch := make([]models.MainProduct, 0, 300)
	invalidProducts := make([]models.InvalidProduct, 0, len(req.Invalid)+64)
	result.TotalRecords = len(req.Invalid)
	for _, invalid := range req.Invalid {
		invalidProducts = append(invalidProducts, newInvalidProduct(
			runID,
			invalid.SourceURL,
			"parser-local-sync",
			invalid.Error,
			invalid.Payload,
		))
	}
	result.InvalidCount = len(invalidProducts)

	flush := func() error {
		if req.SyncEksmo && len(eksmoBatch) > 0 {
			upserted, modified, skipped, upsertErr := s.eksmoRepo.UpsertBatch(ctx, eksmoBatch)
			if upsertErr != nil {
				return upsertErr
			}
			result.EksmoUpserted += upserted
			result.EksmoModified += modified
			result.EksmoSkipped += skipped
			eksmoBatch = eksmoBatch[:0]
		}
		if req.SyncMain && len(mainBatch) > 0 {
			inserted, modified, skipped, upsertErr := s.mainRepo.UpsertImported(ctx, mainBatch)
			if upsertErr != nil {
				return upsertErr
			}
			result.MainInserted += inserted
			result.MainModified += modified
			result.MainSkipped += skipped
			mainBatch = mainBatch[:0]
		}
		return nil
	}

	for _, record := range req.Records {
		source := map[string]any{}
		for key, value := range record.Data {
			source[key] = value
		}
		if strings.TrimSpace(record.SourceURL) != "" {
			source["source_url"] = strings.TrimSpace(record.SourceURL)
		}

		result.TotalRecords++

		eksmoMapped := applyRules(source, req.Rules, "eksmo.")
		mainMapped := applyRules(source, req.Rules, "main.")

		parsedEksmo, hasEksmo := buildEksmoProductFromMapping(eksmoMapped, source, runID)
		if req.SyncEksmo {
			if !hasEksmo {
				result.EksmoSkipped++
			} else {
				eksmoBatch = append(eksmoBatch, parsedEksmo)
			}
		}

		if req.SyncMain {
			parsedMain, hasMain := buildMainProductFromMapping(mainMapped, source, parsedEksmo)
			if !hasMain {
				result.MainSkipped++
			} else {
				mainBatch = append(mainBatch, parsedMain)
			}
			if !hasEksmo && !hasMain {
				invalidProducts = append(invalidProducts, newInvalidProduct(
					runID,
					strings.TrimSpace(record.SourceURL),
					"parser-local-sync",
					"record did not produce a valid eksmo or main product",
					source,
				))
				result.InvalidCount++
			}
		} else if !hasEksmo {
			invalidProducts = append(invalidProducts, newInvalidProduct(
				runID,
				strings.TrimSpace(record.SourceURL),
				"parser-local-sync",
				"record did not produce a valid eksmo product",
				source,
			))
			result.InvalidCount++
		}

		if len(eksmoBatch) >= 250 || len(mainBatch) >= 250 {
			if err := flush(); err != nil {
				return result, err
			}
		}
	}

	if err := flush(); err != nil {
		return result, err
	}
	if err := s.saveInvalidProducts(ctx, invalidProducts); err != nil {
		return result, err
	}

	return result, nil
}

func (s *ParserAppService) saveInvalidProducts(ctx context.Context, products []models.InvalidProduct) error {
	if len(products) == 0 || s.invalidRepo == nil {
		return nil
	}
	return s.invalidRepo.InsertMany(ctx, products)
}

func (s *ParserAppService) discoverCandidateURLs(
	ctx context.Context,
	sourceURL string,
	limit int,
	maxSitemaps int,
	rps float64,
) ([]string, error) {
	unlimited := limit <= 0
	maxURLs := limit * 200
	if unlimited {
		maxURLs = parserMaxDiscoveredURLs
	} else {
		if maxURLs < 1200 {
			maxURLs = 1200
		}
		if maxURLs > parserMaxDiscoveredURLs {
			maxURLs = parserMaxDiscoveredURLs
		}
	}

	candidateSitemaps := buildSitemapCandidates(sourceURL)
	limiter := newHostRateLimiter(rps)

	seenSitemaps := map[string]struct{}{}
	seenURLs := map[string]struct{}{}

	if apiURLs, apiErr := s.discoverBookUZCatalogURLs(ctx, sourceURL, limit, maxURLs, limiter); apiErr == nil {
		for _, item := range apiURLs {
			if item == "" {
				continue
			}
			seenURLs[item] = struct{}{}
			if len(seenURLs) >= maxURLs {
				break
			}
		}
	}

	queue := append([]string{}, candidateSitemaps...)

	for len(queue) > 0 && len(seenSitemaps) < maxSitemaps && len(seenURLs) < maxURLs {
		sitemapURL := queue[0]
		queue = queue[1:]
		if _, exists := seenSitemaps[sitemapURL]; exists {
			continue
		}
		seenSitemaps[sitemapURL] = struct{}{}

		body, contentType, _, _, fetchErr := s.fetchBody(ctx, sitemapURL, limiter)
		if fetchErr != nil {
			continue
		}
		if !looksLikeSitemapDocument(sitemapURL, contentType, body) {
			continue
		}

		children, urls, parseErr := parseSitemapDocument(body)
		if parseErr != nil {
			continue
		}
		for _, child := range children {
			child = strings.TrimSpace(child)
			if child == "" {
				continue
			}
			if _, exists := seenSitemaps[child]; exists {
				continue
			}
			queue = append(queue, child)
		}
		for _, pageURL := range urls {
			pageURL = strings.TrimSpace(pageURL)
			if pageURL == "" {
				continue
			}
			seenURLs[pageURL] = struct{}{}
			if len(seenURLs) >= maxURLs {
				break
			}
		}
	}

	// Always keep source URL in candidate pool.
	seenURLs[sourceURL] = struct{}{}

	// Automatic fallback: crawl the site directly and detect product pages
	// when sitemap is absent, too small, or dominated by non-product URLs.
	minCandidates := limit * 20
	if unlimited {
		minCandidates = 400
	} else if minCandidates < 80 {
		minCandidates = 80
	}
	if minCandidates > maxURLs {
		minCandidates = maxURLs
	}
	minProductCandidates := limit
	if unlimited {
		minProductCandidates = 50
	} else if minProductCandidates < 5 {
		minProductCandidates = 5
	}
	if minProductCandidates > maxURLs {
		minProductCandidates = maxURLs
	}
	productLikeCandidates := countLikelyProductCandidates(seenURLs)
	if len(seenURLs) < minCandidates || productLikeCandidates < minProductCandidates {
		crawlMaxPages := limit * 8
		if unlimited {
			crawlMaxPages = 8000
		} else if crawlMaxPages < 60 {
			crawlMaxPages = 60
		}
		if crawlMaxPages > 250000 {
			crawlMaxPages = 250000
		}
		crawlCollect := maxURLs - len(seenURLs)
		if crawlCollect < minProductCandidates {
			crawlCollect = minProductCandidates
		}
		if crawlCollect < 100 {
			crawlCollect = 100
		}

		crawledURLs, crawlErr := s.discoverBySiteCrawl(
			ctx,
			sourceURL,
			limiter,
			crawlMaxPages,
			4,
			crawlCollect,
		)
		if crawlErr == nil {
			for _, item := range crawledURLs {
				if item == "" {
					continue
				}
				seenURLs[item] = struct{}{}
				if len(seenURLs) >= maxURLs {
					break
				}
			}
		}
	}

	if len(seenURLs) == 0 {
		return []string{sourceURL}, nil
	}

	allURLs := make([]string, 0, len(seenURLs))
	for rawURL := range seenURLs {
		allURLs = append(allURLs, rawURL)
	}

	sort.SliceStable(allURLs, func(i, j int) bool {
		iLikely := isLikelyProductPath(allURLs[i]) || scoreProductURL(allURLs[i]) >= 18
		jLikely := isLikelyProductPath(allURLs[j]) || scoreProductURL(allURLs[j]) >= 18
		if iLikely != jLikely {
			return iLikely
		}

		iScore := scoreCandidateURLForParsing(allURLs[i])
		jScore := scoreCandidateURLForParsing(allURLs[j])
		if iScore == jScore {
			return allURLs[i] < allURLs[j]
		}
		return iScore > jScore
	})

	maxReturn := limit * 120
	if unlimited {
		maxReturn = maxURLs
	} else {
		if maxReturn < 600 {
			maxReturn = 600
		}
		if maxReturn > maxURLs {
			maxReturn = maxURLs
		}
	}
	if len(allURLs) > maxReturn {
		allURLs = allURLs[:maxReturn]
	}
	return allURLs, nil
}

type crawlQueueItem struct {
	URL   string
	Depth int
	Score int
}

func (s *ParserAppService) discoverBySiteCrawl(
	ctx context.Context,
	sourceURL string,
	limiter *hostRateLimiter,
	maxPages int,
	maxDepth int,
	maxCollect int,
) ([]string, error) {
	if maxPages < 1 {
		maxPages = 120
	}
	if maxDepth < 1 {
		maxDepth = 3
	}
	if maxCollect < 1 {
		maxCollect = 300
	}

	parsed, err := url.Parse(sourceURL)
	if err != nil {
		return nil, err
	}
	baseHost := normalizeHost(parsed.Host)
	if baseHost == "" {
		return nil, errors.New("invalid source host")
	}

	seeds := buildCrawlerSeeds(sourceURL)
	queue := make([]crawlQueueItem, 0, len(seeds))
	queued := map[string]struct{}{}
	for _, seed := range seeds {
		normalized, ok := normalizeCrawlURL(seed, sourceURL, baseHost)
		if !ok {
			continue
		}
		if _, exists := queued[normalized]; exists {
			continue
		}
		queued[normalized] = struct{}{}
		queue = append(queue, crawlQueueItem{
			URL:   normalized,
			Depth: 0,
			Score: scoreDiscoveryLink(normalized, 0),
		})
	}

	visited := map[string]struct{}{}
	productSet := map[string]struct{}{}
	likelySet := map[string]struct{}{}
	likelyScore := map[string]int{}

	for len(queue) > 0 && len(visited) < maxPages && (len(productSet)+len(likelySet)) < maxCollect {
		select {
		case <-ctx.Done():
			return makeCollectedURLs(productSet, likelySet, likelyScore, maxCollect), nil
		default:
		}

		next := popBestCrawlItem(&queue)
		if next.URL == "" {
			break
		}
		if _, exists := visited[next.URL]; exists {
			continue
		}
		visited[next.URL] = struct{}{}

		body, contentType, status, _, fetchErr := s.fetchBody(ctx, next.URL, limiter)
		if fetchErr != nil || status >= 400 {
			continue
		}

		if data, isProduct := extractProductData(next.URL, body); isProduct {
			source := toString(data["source_url"])
			if source == "" {
				source = next.URL
			}
			productSet[source] = struct{}{}
		} else if isLikelyProductPath(next.URL) || scoreProductURL(next.URL) >= 14 {
			likelySet[next.URL] = struct{}{}
			likelyScore[next.URL] = scoreProductURL(next.URL)
		}

		if next.Depth >= maxDepth {
			continue
		}
		if !looksLikeHTMLDocument(contentType, body) {
			continue
		}

		links := extractNavigableLinks(next.URL, body, baseHost)
		links = append(links, extractEmbeddedProductURLs(next.URL, body, baseHost)...)
		for _, link := range links {
			if _, exists := visited[link]; exists {
				continue
			}
			if _, exists := queued[link]; exists {
				continue
			}
			score := scoreDiscoveryLink(link, next.Depth+1)
			if score < -2 {
				continue
			}
			queued[link] = struct{}{}
			queue = append(queue, crawlQueueItem{
				URL:   link,
				Depth: next.Depth + 1,
				Score: score,
			})

			if score >= 8 && (isLikelyProductPath(link) || scoreProductURL(link) >= 12) {
				likelySet[link] = struct{}{}
				if _, exists := likelyScore[link]; !exists || likelyScore[link] < score {
					likelyScore[link] = score
				}
			}
		}
	}

	return makeCollectedURLs(productSet, likelySet, likelyScore, maxCollect), nil
}

func makeCollectedURLs(
	productSet map[string]struct{},
	likelySet map[string]struct{},
	likelyScore map[string]int,
	maxCollect int,
) []string {
	if maxCollect < 1 {
		maxCollect = 300
	}

	productURLs := make([]string, 0, len(productSet))
	for item := range productSet {
		productURLs = append(productURLs, item)
	}
	sort.SliceStable(productURLs, func(i, j int) bool {
		iScore := scoreProductURL(productURLs[i])
		jScore := scoreProductURL(productURLs[j])
		if iScore == jScore {
			return productURLs[i] < productURLs[j]
		}
		return iScore > jScore
	})

	result := make([]string, 0, maxCollect)
	seen := map[string]struct{}{}
	for _, item := range productURLs {
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
		if len(result) >= maxCollect {
			return result
		}
	}

	likelyURLs := make([]string, 0, len(likelySet))
	for item := range likelySet {
		if _, exists := seen[item]; exists {
			continue
		}
		likelyURLs = append(likelyURLs, item)
	}
	sort.SliceStable(likelyURLs, func(i, j int) bool {
		iScore := likelyScore[likelyURLs[i]]
		jScore := likelyScore[likelyURLs[j]]
		if iScore == jScore {
			iScore = scoreProductURL(likelyURLs[i])
			jScore = scoreProductURL(likelyURLs[j])
		}
		if iScore == jScore {
			return likelyURLs[i] < likelyURLs[j]
		}
		return iScore > jScore
	})

	for _, item := range likelyURLs {
		result = append(result, item)
		if len(result) >= maxCollect {
			break
		}
	}
	return result
}

func popBestCrawlItem(items *[]crawlQueueItem) crawlQueueItem {
	if items == nil || len(*items) == 0 {
		return crawlQueueItem{}
	}
	bestIndex := 0
	best := (*items)[0]
	for idx := 1; idx < len(*items); idx++ {
		current := (*items)[idx]
		if current.Score > best.Score || (current.Score == best.Score && current.Depth < best.Depth) {
			best = current
			bestIndex = idx
		}
	}
	lastIndex := len(*items) - 1
	(*items)[bestIndex] = (*items)[lastIndex]
	*items = (*items)[:lastIndex]
	return best
}

func buildCrawlerSeeds(sourceURL string) []string {
	candidates := []string{}
	add := func(item string) {
		item = strings.TrimSpace(item)
		if item == "" {
			return
		}
		for _, existing := range candidates {
			if existing == item {
				return
			}
		}
		candidates = append(candidates, item)
	}

	add(sourceURL)
	parsed, err := url.Parse(sourceURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return candidates
	}

	base := parsed.Scheme + "://" + parsed.Host
	add(base)
	add(base + "/catalog")
	add(base + "/catalogue")
	add(base + "/catalog/all")
	add(base + "/books")
	add(base + "/book")
	add(base + "/products")
	add(base + "/search")
	add(base + "/shop")
	add(base + "/shop/all")
	return candidates
}

func looksLikeHTMLDocument(contentType string, body []byte) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if strings.Contains(contentType, "text/html") || strings.Contains(contentType, "application/xhtml+xml") {
		return true
	}
	if strings.Contains(contentType, "xml") {
		return false
	}
	peek := strings.ToLower(strings.TrimSpace(string(body[:minInt(len(body), 400)])))
	return strings.Contains(peek, "<html") || strings.Contains(peek, "<head") || strings.Contains(peek, "<body")
}

func extractNavigableLinks(baseURL string, body []byte, expectedHost string) []string {
	root, err := xhtml.Parse(bytes.NewReader(body))
	if err != nil {
		return nil
	}

	seen := map[string]struct{}{}
	result := make([]string, 0, 64)
	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode || !strings.EqualFold(node.Data, "a") {
			return
		}
		href := strings.TrimSpace(getAttr(node, "href"))
		if href == "" {
			return
		}
		normalized, ok := normalizeCrawlURL(href, baseURL, expectedHost)
		if !ok {
			return
		}
		if _, exists := seen[normalized]; exists {
			return
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	})
	return result
}

var embeddedAbsoluteProductURLPattern = regexp.MustCompile(`(?i)https?://[^"'\s<>]+/(product|products|item|dp|book|books)/[^"'\s<>]+`)
var embeddedRelativeProductPathPattern = regexp.MustCompile(`(?i)/(product|products|item|dp|book|books)/[a-z0-9%._~\-/]+`)
var embeddedBareRelativeProductPathPattern = regexp.MustCompile(`(?i)(product|products|item|dp|book|books)/[a-z0-9%._~\-/]+`)
var embeddedCatalogNumericProductPathPattern = regexp.MustCompile(`(?i)/catalog/[0-9]{2,}/[0-9]{3,}[a-z0-9%._~\-/]*`)
var embeddedEscapedRelativeProductPathPattern = regexp.MustCompile(`(?i)\\/(product|products|item|dp|book|books)\\/[a-z0-9%._~\\\-/]+`)
var embeddedEscapedBareRelativeProductPathPattern = regexp.MustCompile(`(?i)(product|products|item|dp|book|books)\\/[a-z0-9%._~\\\-/]+`)
var embeddedEscapedCatalogNumericProductPathPattern = regexp.MustCompile(`(?i)\\/catalog\\/[0-9]{2,}\\/[0-9]{3,}[a-z0-9%._~\\\-/]*`)
var bareRelativeProductPathPattern = regexp.MustCompile(`(?i)^(product|products|item|dp|book|books)/[a-z0-9%._~\-/]+$`)

func extractEmbeddedProductURLs(baseURL string, body []byte, expectedHost string) []string {
	if len(body) == 0 {
		return nil
	}
	peekSize := minInt(len(body), 3*1024*1024)
	chunk := string(body[:peekSize])
	seen := map[string]struct{}{}
	result := make([]string, 0, 32)
	add := func(raw string) {
		raw = strings.TrimSpace(strings.Trim(raw, `"'`))
		if raw == "" {
			return
		}
		raw = strings.ReplaceAll(raw, `\/`, "/")
		if bareRelativeProductPathPattern.MatchString(raw) {
			raw = "/" + raw
		}
		normalized, ok := normalizeCrawlURL(raw, baseURL, expectedHost)
		if !ok || !isLikelyProductPath(normalized) {
			return
		}
		if _, exists := seen[normalized]; exists {
			return
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}

	for _, item := range embeddedAbsoluteProductURLPattern.FindAllString(chunk, -1) {
		add(item)
	}
	for _, item := range embeddedRelativeProductPathPattern.FindAllString(chunk, -1) {
		add(item)
	}
	for _, item := range embeddedBareRelativeProductPathPattern.FindAllString(chunk, -1) {
		add(item)
	}
	for _, item := range embeddedCatalogNumericProductPathPattern.FindAllString(chunk, -1) {
		add(item)
	}
	for _, item := range embeddedEscapedRelativeProductPathPattern.FindAllString(chunk, -1) {
		add(item)
	}
	for _, item := range embeddedEscapedBareRelativeProductPathPattern.FindAllString(chunk, -1) {
		add(item)
	}
	for _, item := range embeddedEscapedCatalogNumericProductPathPattern.FindAllString(chunk, -1) {
		add(item)
	}
	return result
}

func normalizeCrawlURL(rawURL string, baseURL string, expectedHost string) (string, bool) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", false
	}
	lower := strings.ToLower(rawURL)
	if strings.HasPrefix(lower, "#") || strings.HasPrefix(lower, "mailto:") || strings.HasPrefix(lower, "tel:") || strings.HasPrefix(lower, "javascript:") {
		return "", false
	}

	base, err := url.Parse(baseURL)
	if err != nil {
		return "", false
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", false
	}
	resolved := base.ResolveReference(parsed)
	if resolved == nil {
		return "", false
	}
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return "", false
	}
	if expectedHost != "" && normalizeHost(resolved.Host) != normalizeHost(expectedHost) {
		return "", false
	}
	resolved.Fragment = ""
	if hasBlockedPath(resolved.Path) {
		return "", false
	}
	return resolved.String(), true
}

func normalizeHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	host = strings.TrimPrefix(host, "www.")
	return host
}

func hasBlockedPath(pathValue string) bool {
	pathLower := strings.ToLower(strings.TrimSpace(pathValue))
	for _, blocked := range []string{
		".jpg", ".jpeg", ".png", ".gif", ".svg", ".webp",
		".css", ".js", ".json", ".xml", ".pdf", ".zip", ".mp4", ".mp3",
	} {
		if strings.HasSuffix(pathLower, blocked) {
			return true
		}
	}
	return false
}

func scoreDiscoveryLink(rawURL string, depth int) int {
	score := scoreProductURL(rawURL)
	urlLower := strings.ToLower(rawURL)

	for _, token := range []string{"catalog", "books", "book", "product", "shop", "store"} {
		if strings.Contains(urlLower, token) {
			score += 2
		}
	}
	for _, bad := range []string{"login", "account", "basket", "cart", "wishlist", "compare", "faq", "help", "privacy", "terms"} {
		if strings.Contains(urlLower, bad) {
			score -= 6
		}
	}
	for _, bad := range []string{"article", "articles", "/news/", "/blog/", "/author/", "/authors/"} {
		if strings.Contains(urlLower, bad) {
			score -= 10
		}
	}
	if isLikelyProductPath(rawURL) {
		score += 8
	}
	if depth <= 1 {
		score += 2
	} else {
		score -= depth
	}
	return score
}

func (s *ParserAppService) parseURLs(
	ctx context.Context,
	runID primitive.ObjectID,
	urls []string,
	workers int,
	rps float64,
	targetCount int,
) (parserURLParseResult, error) {
	result := parserURLParseResult{}
	if len(urls) == 0 {
		return result, nil
	}
	if targetCount < 0 {
		targetCount = 0
	}
	if targetCount > parserMaxProducts {
		targetCount = parserMaxProducts
	}

	parseCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	limiter := newHostRateLimiter(rps)
	type parseItem struct {
		record models.ParserRecord
		ok     bool
	}

	jobs := make(chan string, maxInt(workers*2, 8))
	results := make(chan parseItem, maxInt(workers*8, 64))
	var wg sync.WaitGroup
	var retryMu sync.Mutex
	totalRetries := 0

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			emit := func(item parseItem) bool {
				select {
				case results <- item:
					return true
				case <-parseCtx.Done():
					return false
				}
			}
			for currentURL := range jobs {
				body, _, status, retries, err := s.fetchBody(parseCtx, currentURL, limiter)
				if retries > 0 {
					retryMu.Lock()
					totalRetries += retries
					retryMu.Unlock()
				}
				if err != nil || status >= 400 {
					if !emit(parseItem{ok: false}) {
						return
					}
					continue
				}

				data, ok := extractProductData(currentURL, body)
				if !ok {
					parsedURL, parseErr := url.Parse(currentURL)
					expectedHost := ""
					if parseErr == nil {
						expectedHost = normalizeHost(parsedURL.Host)
					}
					fallbackURLs := extractEmbeddedProductURLs(currentURL, body, expectedHost)
					if len(fallbackURLs) > 0 {
						fallbackLimit := 12
						if targetCount > 0 && targetCount*4 > fallbackLimit {
							fallbackLimit = targetCount * 4
						}
						if fallbackLimit > 64 {
							fallbackLimit = 64
						}
						for idx, fallbackURL := range fallbackURLs {
							if idx >= fallbackLimit {
								break
							}

							fallbackBody, _, fallbackStatus, fallbackRetries, fallbackErr := s.fetchBody(parseCtx, fallbackURL, limiter)
							if fallbackRetries > 0 {
								retryMu.Lock()
								totalRetries += fallbackRetries
								retryMu.Unlock()
							}
							if fallbackErr != nil || fallbackStatus >= 400 {
								continue
							}

							fallbackData, fallbackOK := extractProductData(fallbackURL, fallbackBody)
							if !fallbackOK {
								continue
							}

							record := models.ParserRecord{
								RunID:     runID,
								SourceURL: firstNonEmpty(strings.TrimSpace(toString(fallbackData["source_url"])), fallbackURL),
								Data:      fallbackData,
								CreatedAt: time.Now().UTC(),
							}
							if !emit(parseItem{record: record, ok: true}) {
								return
							}
						}
						if !emit(parseItem{ok: false}) {
							return
						}
						continue
					}
					if !emit(parseItem{ok: false}) {
						return
					}
					continue
				}
				record := models.ParserRecord{
					RunID:     runID,
					SourceURL: firstNonEmpty(strings.TrimSpace(toString(data["source_url"])), currentURL),
					Data:      data,
					CreatedAt: time.Now().UTC(),
				}
				if !emit(parseItem{record: record, ok: true}) {
					return
				}
			}
		}()
	}

	go func() {
		for _, item := range urls {
			select {
			case <-parseCtx.Done():
				close(jobs)
				wg.Wait()
				close(results)
				return
			case jobs <- item:
			}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	batch := make([]models.ParserRecord, 0, parserRecordFlushBatch)
	fieldSet := map[string]struct{}{}
	seenSource := map[string]struct{}{}
	var storeErr error

	flushBatch := func() {
		if storeErr != nil || len(batch) == 0 {
			return
		}
		if err := s.parserRepo.AppendRunRecords(ctx, runID, batch); err != nil {
			storeErr = err
			cancel()
			return
		}
		batch = batch[:0]
	}

	for item := range results {
		if storeErr != nil {
			continue
		}
		if !item.ok {
			continue
		}
		if targetCount > 0 && result.ParsedCount >= targetCount {
			cancel()
			continue
		}
		sourceURL := strings.TrimSpace(item.record.SourceURL)
		if sourceURL == "" {
			continue
		}
		if _, exists := seenSource[sourceURL]; exists {
			continue
		}
		seenSource[sourceURL] = struct{}{}

		for key := range item.record.Data {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			fieldSet[key] = struct{}{}
		}

		if len(result.Sample) < parserSampleSize {
			result.Sample = append(result.Sample, item.record)
		}

		batch = append(batch, item.record)
		result.ParsedCount++

		if len(batch) >= parserRecordFlushBatch {
			flushBatch()
		}
		if targetCount > 0 && result.ParsedCount >= targetCount {
			cancel()
		}
	}
	flushBatch()
	result.RateLimitRetries = totalRetries

	if storeErr != nil {
		result.DetectedFields = collectDetectedFieldsFromSet(fieldSet)
		return result, storeErr
	}

	result.DetectedFields = collectDetectedFieldsFromSet(fieldSet)
	return result, nil
}

func (s *ParserAppService) detectSourceBlocking(ctx context.Context, sourceURL string) error {
	probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	body, contentType, status, _, err := s.fetchBody(probeCtx, sourceURL, nil)
	if err != nil {
		reason := detectFetchErrorReason(err)
		if reason == "" {
			return nil
		}
		return fmt.Errorf("source website is not reachable from server-side parser (%s). Use desktop parser for this source or configure a trusted outbound proxy", reason)
	}
	reason := detectBotProtectionReason(status, contentType, body)
	if reason == "" {
		return nil
	}

	return fmt.Errorf("source website is protected from server-side parsing (%s). Use desktop parser for this source or configure a trusted outbound proxy", reason)
}

func detectFetchErrorReason(err error) string {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "request timeout"
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return "request timeout"
		}
		if urlErr.Err != nil {
			msg := strings.TrimSpace(urlErr.Err.Error())
			if msg != "" {
				return msg
			}
		}
	}

	msg := strings.TrimSpace(strings.ToLower(err.Error()))
	if msg == "" {
		return ""
	}
	if strings.Contains(msg, "timeout") {
		return "request timeout"
	}
	if strings.Contains(msg, "connection refused") {
		return "connection refused"
	}
	if strings.Contains(msg, "no such host") {
		return "dns resolve failed"
	}
	if strings.Contains(msg, "network is unreachable") {
		return "network unreachable"
	}
	if strings.Contains(msg, "tls") {
		return "tls handshake failed"
	}
	return msg
}

func detectBotProtectionReason(statusCode int, contentType string, body []byte) string {
	bodyLower := strings.ToLower(string(body))
	contentTypeLower := strings.ToLower(strings.TrimSpace(contentType))

	type marker struct {
		token  string
		reason string
	}

	markers := []marker{
		{token: "ddos-guard", reason: "ddos-guard"},
		{token: "js-challenge", reason: "ddos-guard challenge"},
		{token: "checking your browser", reason: "anti-bot browser challenge"},
		{token: "cloudflare", reason: "cloudflare challenge"},
		{token: "cf-chl", reason: "cloudflare challenge"},
		{token: "g-recaptcha", reason: "captcha challenge"},
		{token: "hcaptcha", reason: "captcha challenge"},
		{token: "captcha challenge", reason: "captcha challenge"},
		{token: "access denied", reason: "access denied"},
		{token: "forbidden", reason: "forbidden"},
	}

	for _, item := range markers {
		if strings.Contains(bodyLower, item.token) {
			return item.reason
		}
	}
	if strings.Contains(bodyLower, "captcha") &&
		(strings.Contains(bodyLower, "verify you are human") ||
			strings.Contains(bodyLower, "подтвердите, что вы человек") ||
			strings.Contains(bodyLower, "i'm not a robot")) {
		return "captcha challenge"
	}

	if statusCode == http.StatusForbidden || statusCode == http.StatusTooManyRequests || statusCode == http.StatusServiceUnavailable {
		if strings.Contains(contentTypeLower, "text/html") || strings.Contains(contentTypeLower, "text/plain") {
			return fmt.Sprintf("http %d", statusCode)
		}
	}

	return ""
}

func (s *ParserAppService) fetchBody(
	ctx context.Context,
	rawURL string,
	limiter *hostRateLimiter,
) ([]byte, string, int, int, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return nil, "", 0, 0, err
	}

	host := parsedURL.Host
	if host == "" {
		return nil, "", 0, 0, errors.New("invalid host")
	}
	hostname := strings.ToLower(parsedURL.Hostname())
	enableMirrorFallback := shouldUseMirrorFallback(hostname)

	maxAttempts := 3
	retries := 0
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if limiter != nil {
			limiter.Wait(host)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, "", 0, retries, err
		}
		req.Header.Set("User-Agent", s.userAgent)
		req.Header.Set("Accept", "text/html,application/xml,text/xml,application/json;q=0.9,*/*;q=0.8")
		req.Header.Set("Accept-Language", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Pragma", "no-cache")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			if attempt == maxAttempts {
				if enableMirrorFallback {
					return s.fetchViaMirrorProxy(ctx, rawURL, limiter, retries)
				}
				return nil, "", 0, retries, err
			}
			retries++
			time.Sleep(backoffDuration(attempt, true))
			continue
		}

		if shouldRetryStatus(resp.StatusCode) {
			wait := backoffDuration(attempt, resp.StatusCode == http.StatusTooManyRequests)
			if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After")); retryAfter > wait {
				wait = retryAfter
			}
			_ = resp.Body.Close()
			if attempt == maxAttempts {
				return nil, resp.Header.Get("Content-Type"), resp.StatusCode, retries, fmt.Errorf("http status %d", resp.StatusCode)
			}
			retries++
			time.Sleep(wait)
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
		_ = resp.Body.Close()
		if readErr != nil {
			if attempt == maxAttempts {
				if enableMirrorFallback {
					return s.fetchViaMirrorProxy(ctx, rawURL, limiter, retries)
				}
				return nil, resp.Header.Get("Content-Type"), resp.StatusCode, retries, readErr
			}
			retries++
			time.Sleep(backoffDuration(attempt, false))
			continue
		}
		if isRateLimitedBody(resp.StatusCode, resp.Header.Get("Content-Type"), body) {
			wait := backoffDuration(attempt, true)
			if attempt == maxAttempts {
				return nil, resp.Header.Get("Content-Type"), http.StatusTooManyRequests, retries, errors.New("rate limited")
			}
			retries++
			time.Sleep(wait)
			continue
		}
		if enableMirrorFallback {
			reason := detectBotProtectionReason(resp.StatusCode, resp.Header.Get("Content-Type"), body)
			if reason != "" {
				return s.fetchViaMirrorProxy(ctx, rawURL, limiter, retries)
			}
		}
		return body, resp.Header.Get("Content-Type"), resp.StatusCode, retries, nil
	}

	return nil, "", 0, retries, errors.New("request attempts exhausted")
}

func shouldUseMirrorFallback(hostname string) bool {
	host := strings.ToLower(strings.TrimSpace(hostname))
	if host == "" {
		return false
	}
	return host == "chitai-gorod.ru" || strings.HasSuffix(host, ".chitai-gorod.ru")
}

func (s *ParserAppService) fetchViaMirrorProxy(
	ctx context.Context,
	rawURL string,
	limiter *hostRateLimiter,
	retries int,
) ([]byte, string, int, int, error) {
	const proxyHost = "api.codetabs.com"
	proxyURL := "https://api.codetabs.com/v1/proxy/?quest=" + url.QueryEscape(rawURL)

	maxAttempts := 2
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if limiter != nil {
			limiter.Wait(proxyHost)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, proxyURL, nil)
		if err != nil {
			return nil, "", 0, retries, err
		}
		req.Header.Set("User-Agent", s.userAgent)
		req.Header.Set("Accept", "text/html,application/xml,text/xml,application/json;q=0.9,*/*;q=0.8")
		req.Header.Set("Accept-Language", "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7")
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Pragma", "no-cache")

		resp, err := s.httpClient.Do(req)
		if err != nil {
			if attempt == maxAttempts {
				return nil, "", 0, retries, err
			}
			retries++
			time.Sleep(backoffDuration(attempt, true))
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
		_ = resp.Body.Close()
		if readErr != nil {
			if attempt == maxAttempts {
				return nil, resp.Header.Get("Content-Type"), resp.StatusCode, retries, readErr
			}
			retries++
			time.Sleep(backoffDuration(attempt, false))
			continue
		}
		if isRateLimitedBody(resp.StatusCode, resp.Header.Get("Content-Type"), body) {
			wait := backoffDuration(attempt, true)
			if attempt == maxAttempts {
				return nil, resp.Header.Get("Content-Type"), http.StatusTooManyRequests, retries, errors.New("proxy rate limited")
			}
			retries++
			time.Sleep(wait)
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			if attempt == maxAttempts {
				return nil, resp.Header.Get("Content-Type"), resp.StatusCode, retries, fmt.Errorf("proxy status %d", resp.StatusCode)
			}
			retries++
			time.Sleep(backoffDuration(attempt, false))
			continue
		}

		return body, resp.Header.Get("Content-Type"), resp.StatusCode, retries, nil
	}

	return nil, "", 0, retries, errors.New("mirror proxy attempts exhausted")
}

func shouldRetryStatus(status int) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	return status == http.StatusBadGateway || status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout || status == http.StatusInternalServerError
}

func backoffDuration(attempt int, strong bool) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	base := 400 * time.Millisecond
	if strong {
		base = 900 * time.Millisecond
	}
	factor := math.Pow(2, float64(attempt-1))
	jitter := time.Duration(rand.Intn(250)) * time.Millisecond
	wait := time.Duration(float64(base) * factor)
	if wait > 12*time.Second {
		wait = 12 * time.Second
	}
	return wait + jitter
}

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		wait := time.Until(retryAt)
		if wait > 0 {
			return wait
		}
	}
	return 0
}

func isRateLimitedBody(statusCode int, contentType string, body []byte) bool {
	if statusCode == http.StatusTooManyRequests {
		return true
	}

	ct := strings.ToLower(strings.TrimSpace(contentType))
	if ct != "" && !strings.Contains(ct, "text/html") && !strings.Contains(ct, "text/plain") {
		return false
	}

	preview := strings.ToLower(string(body))
	if len(preview) > 32*1024 {
		preview = preview[:32*1024]
	}

	if strings.Contains(preview, "too many requests") {
		return true
	}
	if strings.Contains(preview, "you've made too many requests") {
		return true
	}
	if strings.Contains(preview, "rate limit exceeded") {
		return true
	}
	if strings.Contains(preview, "429 too many requests") {
		return true
	}
	return false
}

type hostRateLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	next     map[string]time.Time
}

func newHostRateLimiter(requestsPerSec float64) *hostRateLimiter {
	if requestsPerSec <= 0 {
		requestsPerSec = 1
	}
	interval := time.Duration(float64(time.Second) / requestsPerSec)
	if interval < 50*time.Millisecond {
		interval = 50 * time.Millisecond
	}
	return &hostRateLimiter{interval: interval, next: map[string]time.Time{}}
}

func (l *hostRateLimiter) Wait(host string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	now := time.Now()
	nextTime := l.next[host]
	if now.Before(nextTime) {
		time.Sleep(nextTime.Sub(now))
		now = time.Now()
	}
	l.next[host] = now.Add(l.interval)
	l.mu.Unlock()
}

func buildSitemapCandidates(sourceURL string) []string {
	candidates := []string{}
	add := func(item string) {
		item = strings.TrimSpace(item)
		if item == "" {
			return
		}
		for _, existing := range candidates {
			if existing == item {
				return
			}
		}
		candidates = append(candidates, item)
	}

	add(sourceURL)
	parsed, err := url.Parse(sourceURL)
	if err != nil {
		return candidates
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return candidates
	}

	base := parsed.Scheme + "://" + parsed.Host
	add(base + "/sitemap.xml")
	add(base + "/sitemap_index.xml")
	add(base + "/sitemapindex.xml")
	add(base + "/sitemap-books.xml")
	add(base + "/sitemap-products.xml")
	add(base + "/index.php?route=feed/google_sitemap")
	return candidates
}

func isBookUZBooksListingURL(rawURL string) bool {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return false
	}
	if normalizeHost(parsed.Host) != "book.uz" {
		return false
	}
	path := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(parsed.Path)), "/")
	return path == "/books"
}

func (s *ParserAppService) discoverBookUZCatalogURLs(
	ctx context.Context,
	sourceURL string,
	limit int,
	maxURLs int,
	limiter *hostRateLimiter,
) ([]string, error) {
	if !isBookUZBooksListingURL(sourceURL) {
		return nil, nil
	}
	if maxURLs < 1 {
		maxURLs = 1
	}

	target := maxURLs
	if limit > 0 {
		target = maxInt(limit*2, limit+50)
		if target < 120 {
			target = 120
		}
		if target > maxURLs {
			target = maxURLs
		}
	}

	result := make([]string, 0, minInt(target, 8000))
	seen := map[string]struct{}{}
	page := 1
	perPage := 100
	maxPages := 1500
	if target > perPage*maxPages {
		requiredPages := (target + perPage - 1) / perPage
		maxPages = minInt(requiredPages+20, 10000)
	}

	for page <= maxPages && len(result) < target {
		select {
		case <-ctx.Done():
			return result, nil
		default:
		}

		apiURL := fmt.Sprintf("https://backend.book.uz/user-api/book?page=%d&limit=%d", page, perPage)
		body, _, status, _, err := s.fetchBody(ctx, apiURL, limiter)
		if err != nil || status >= 400 {
			if page == 1 {
				return result, err
			}
			break
		}

		items, reportedTotal := parseBookUZBooksAPIResponse(body)
		if len(items) == 0 {
			break
		}

		for _, item := range items {
			slug := cleanText(firstNonEmpty(toString(item["link"]), toString(item["_id"])))
			if slug == "" {
				continue
			}

			detailURL := "https://book.uz/books/details/" + strings.Trim(slug, "/")
			normalized, ok := normalizeCrawlURL(detailURL, sourceURL, "book.uz")
			if !ok {
				continue
			}
			if _, exists := seen[normalized]; exists {
				continue
			}
			seen[normalized] = struct{}{}
			result = append(result, normalized)

			if len(result) >= target {
				break
			}
		}

		if reportedTotal > 0 && len(result) >= reportedTotal {
			break
		}

		page++
	}

	return result, nil
}

func parseBookUZBooksAPIResponse(body []byte) ([]map[string]any, int) {
	if len(body) == 0 {
		return nil, 0
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, 0
	}

	data := toMap(payload["data"])
	if len(data) == 0 {
		return nil, 0
	}

	total := 0
	if parsedTotal := toInt(data["total"]); parsedTotal != nil && *parsedTotal > 0 {
		total = *parsedTotal
	}

	rawItems, ok := data["data"].([]any)
	if !ok || len(rawItems) == 0 {
		return nil, total
	}

	items := make([]map[string]any, 0, len(rawItems))
	for _, raw := range rawItems {
		if item := toMap(raw); len(item) > 0 {
			items = append(items, item)
		}
	}

	return items, total
}

func looksLikeSitemapDocument(rawURL, contentType string, body []byte) bool {
	urlLower := strings.ToLower(strings.TrimSpace(rawURL))
	if strings.Contains(urlLower, "sitemap") || strings.HasSuffix(urlLower, ".xml") || strings.HasSuffix(urlLower, ".xml.gz") {
		return true
	}
	ct := strings.ToLower(contentType)
	if strings.Contains(ct, "xml") {
		return true
	}
	peek := strings.ToLower(strings.TrimSpace(string(body[:minInt(len(body), 300)])))
	return strings.Contains(peek, "<urlset") || strings.Contains(peek, "<sitemapindex")
}

type sitemapURLSet struct {
	URLs []struct {
		Loc string `xml:"loc"`
	} `xml:"url"`
}

type sitemapIndex struct {
	Sitemaps []struct {
		Loc string `xml:"loc"`
	} `xml:"sitemap"`
}

func parseSitemapDocument(body []byte) ([]string, []string, error) {
	if len(body) == 0 {
		return nil, nil, errors.New("empty sitemap body")
	}

	body = bytes.TrimSpace(body)
	if len(body) >= 2 && body[0] == 0x1f && body[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, nil, err
		}
		defer reader.Close()
		inflated, err := io.ReadAll(io.LimitReader(reader, 16*1024*1024))
		if err != nil {
			return nil, nil, err
		}
		body = inflated
	}

	indexDoc := sitemapIndex{}
	if err := xml.Unmarshal(body, &indexDoc); err == nil && len(indexDoc.Sitemaps) > 0 {
		children := make([]string, 0, len(indexDoc.Sitemaps))
		for _, item := range indexDoc.Sitemaps {
			loc := strings.TrimSpace(item.Loc)
			if loc != "" {
				children = append(children, loc)
			}
		}
		return children, nil, nil
	}

	urlDoc := sitemapURLSet{}
	if err := xml.Unmarshal(body, &urlDoc); err == nil && len(urlDoc.URLs) > 0 {
		urls := make([]string, 0, len(urlDoc.URLs))
		for _, item := range urlDoc.URLs {
			loc := strings.TrimSpace(item.Loc)
			if loc != "" {
				urls = append(urls, loc)
			}
		}
		return nil, urls, nil
	}

	return nil, nil, errors.New("unsupported sitemap format")
}

var productURLPattern = regexp.MustCompile(`(?i)(book|books|product|catalog|item|isbn|product_id=|route=product/product|/dp/|/p/)`)
var trailingNumericProductIDPattern = regexp.MustCompile(`(?i)(?:-|/)\d{4,}(?:/|$)`)
var paginationPathPattern = regexp.MustCompile(`(?i)/page/\d+/?(?:$|\?)`)
var catalogProductSlugPattern = regexp.MustCompile(`(?i)/catalog/(book-|item-|product-|kniga-|isbn-|sku-)`)
var booksDetailsProductPathPattern = regexp.MustCompile(`(?i)/books/details/[^/?#]+/?$`)
var booksDeepProductPathPattern = regexp.MustCompile(`(?i)/books/[^/?#]+/[^/?#]+/?$`)
var booksSingleProductPathPattern = regexp.MustCompile(`(?i)/(book|books)/[^/?#]{3,}/?$`)
var catalogDeepProductPathPattern = regexp.MustCompile(`(?i)/catalog/[^/?#]+/[^/?#]+/?$`)
var rootProductSlugPathPattern = regexp.MustCompile(`(?i)^/[a-z0-9][a-z0-9-]{4,}/?$`)
var inlineAttrKeyPattern = regexp.MustCompile(`(?i)[a-zа-яё0-9()\\-\\s]{2,}:`)
var isbnValuePattern = regexp.MustCompile(`(?i)[0-9x][0-9x\\-]{8,20}`)
var numericIDPattern = regexp.MustCompile(`^[0-9]{3,}$`)
var waitingListJSONPattern = regexp.MustCompile(`(?is)let\s+waitingList\s*=\s*(\[[\s\S]*?\]);`)
var nextDataScriptPattern = regexp.MustCompile(`(?is)<script[^>]*id=["']__NEXT_DATA__["'][^>]*>([\s\S]*?)</script>`)

func countLikelyProductCandidates(urls map[string]struct{}) int {
	count := 0
	for item := range urls {
		if isLikelyProductPath(item) || scoreProductURL(item) >= 18 {
			count++
		}
	}
	return count
}

func isLikelyProductPath(rawURL string) bool {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return false
	}
	rawLower := strings.ToLower(rawURL)
	pathLower := strings.ToLower(rawURL)
	if parsed, err := url.Parse(rawURL); err == nil && parsed.Path != "" {
		pathLower = strings.ToLower(parsed.Path)
	}
	if strings.Contains(pathLower, "/catalog/product/") {
		return true
	}
	if booksDetailsProductPathPattern.MatchString(pathLower) {
		return true
	}
	if catalogProductSlugPattern.MatchString(pathLower) {
		return true
	}
	if booksDeepProductPathPattern.MatchString(pathLower) &&
		!strings.Contains(pathLower, "/catalog/books/") &&
		!strings.Contains(pathLower, "/catalogue/books/") {
		return true
	}
	if booksSingleProductPathPattern.MatchString(pathLower) &&
		!strings.Contains(pathLower, "/catalog/books/") &&
		!strings.Contains(pathLower, "/catalogue/books/") {
		return true
	}
	if catalogDeepProductPathPattern.MatchString(pathLower) &&
		!strings.Contains(pathLower, "/filter/") &&
		!strings.Contains(pathLower, "/search/") {
		return true
	}
	for _, blocked := range []string{
		"/catalog/",
		"/catalogue/",
		"/category/",
		"/categories/",
		"/collection/",
		"/collections/",
		"/articles/",
		"/article/",
		"/news/",
		"/blog/",
		"/author/",
		"/authors/",
		"/brand/",
		"/brands/",
		"/publisher/",
		"/publishers/",
	} {
		if strings.Contains(pathLower, blocked) {
			return false
		}
	}
	if strings.Contains(rawLower, "route=product/product") || strings.Contains(rawLower, "product_id=") {
		return true
	}
	for _, token := range []string{
		"/product/",
		"/products/",
		"/item/",
		"/dp/",
		"/sku/",
		"/isbn/",
		"/gtin/",
	} {
		if strings.Contains(pathLower, token) {
			return true
		}
	}
	if strings.Contains(pathLower, "/book/") && trailingNumericProductIDPattern.MatchString(pathLower) {
		return true
	}
	if strings.Contains(pathLower, "/p/") && trailingNumericProductIDPattern.MatchString(pathLower) {
		return true
	}
	return isRootProductSlugPath(pathLower)
}

func scoreProductURL(rawURL string) int {
	score := 0
	urlLower := strings.ToLower(rawURL)
	pathLower := strings.ToLower(rawURL)
	if parsed, err := url.Parse(rawURL); err == nil && parsed.Path != "" {
		pathLower = strings.ToLower(parsed.Path)
	}
	if productURLPattern.MatchString(urlLower) {
		score += 3
	}
	if strings.Contains(urlLower, "/catalog/product/") {
		score += 18
	}
	if booksDetailsProductPathPattern.MatchString(pathLower) {
		score += 14
	}
	if catalogProductSlugPattern.MatchString(urlLower) {
		score += 18
	}
	if booksDeepProductPathPattern.MatchString(urlLower) &&
		!strings.Contains(urlLower, "/catalog/books/") &&
		!strings.Contains(urlLower, "/catalogue/books/") {
		score += 7
	}
	if booksSingleProductPathPattern.MatchString(pathLower) &&
		!strings.Contains(pathLower, "/catalog/books/") &&
		!strings.Contains(pathLower, "/catalogue/books/") {
		score += 4
	}
	if catalogDeepProductPathPattern.MatchString(urlLower) {
		score += 8
	}
	if strings.Contains(urlLower, "/product/") || strings.Contains(urlLower, "/products/") {
		score += 16
	}
	if strings.Contains(urlLower, "route=product/product") || strings.Contains(urlLower, "product_id=") {
		score += 16
	}
	if (strings.Contains(urlLower, "/book/") || strings.Contains(urlLower, "/books/")) &&
		!strings.Contains(urlLower, "/catalog/books/") &&
		!strings.Contains(urlLower, "/catalogue/books/") {
		score += 3
	}
	if strings.Contains(urlLower, "/item/") || strings.Contains(urlLower, "/dp/") {
		score += 8
	}
	if strings.Contains(urlLower, "isbn") {
		score += 5
	}
	// Demote category/listing urls so small limits still include actual products.
	for _, token := range []string{
		"/catalogue/",
		"/catalog/books/",
		"/catalogue/books/",
		"/category/",
		"/categories/",
		"/collection/",
		"/collections/",
		"/articles/",
		"/article/",
		"/bestseller",
		"/novinki",
		"/new/",
		"/authors/",
		"/gallery/",
		"/galleries/",
		"/blog/",
		"/news/",
		"/search",
	} {
		if strings.Contains(urlLower, token) {
			score -= 8
		}
	}
	if strings.Contains(urlLower, "/catalog/") &&
		!strings.Contains(urlLower, "/catalog/product/") &&
		!catalogProductSlugPattern.MatchString(urlLower) &&
		!catalogDeepProductPathPattern.MatchString(urlLower) {
		score -= 8
	}
	if isRootProductSlugPath(pathLower) {
		score += 9
	}
	if isBlockedRootSlugPath(pathLower) {
		score -= 10
	}
	if strings.Contains(urlLower, "?") {
		score -= 1
	}
	if strings.HasSuffix(urlLower, ".jpg") || strings.HasSuffix(urlLower, ".png") {
		score -= 10
	}
	if strings.Count(urlLower, "/") >= 4 {
		score += 2
	}
	return score
}

func scoreCandidateURLForParsing(rawURL string) int {
	urlLower := strings.ToLower(strings.TrimSpace(rawURL))
	score := scoreProductURL(rawURL)

	if isLikelyProductPath(rawURL) {
		score += 20
	}
	if looksLikeProductURL(urlLower) {
		score += 6
	}
	if isExplicitNonProductPage(urlLower, "") {
		score -= 30
	}
	for _, token := range []string{
		"/articles/",
		"/article/",
		"/blog/",
		"/news/",
		"/author/",
		"/authors/",
		"/category/",
		"/categories/",
		"/collection/",
		"/collections/",
	} {
		if strings.Contains(urlLower, token) {
			score -= 25
			break
		}
	}
	return score
}

func isRootProductSlugPath(pathLower string) bool {
	pathLower = strings.TrimSpace(strings.ToLower(pathLower))
	if pathLower == "" {
		return false
	}
	if !rootProductSlugPathPattern.MatchString(pathLower) {
		return false
	}
	if isBlockedRootSlugPath(pathLower) {
		return false
	}
	return true
}

func isBlockedRootSlugPath(pathLower string) bool {
	trimmed := strings.Trim(strings.ToLower(strings.TrimSpace(pathLower)), "/")
	if trimmed == "" {
		return true
	}
	for _, blocked := range []string{
		"about",
		"contacts",
		"contact",
		"blog",
		"news",
		"catalog",
		"catalogue",
		"categories",
		"category",
		"collection",
		"collections",
		"search",
		"shop",
		"cart",
		"checkout",
		"profile",
		"login",
		"account",
		"privacy",
		"terms",
		"authors",
		"author",
		"company",
		"press",
		"faq",
		"delivery",
		"payment",
		"posters",
		"discount",
		"favorites",
		"orders",
	} {
		if trimmed == blocked {
			return true
		}
	}
	return false
}

func extractProductData(pageURL string, body []byte) (map[string]any, bool) {
	root, err := xhtml.Parse(bytes.NewReader(body))
	if err != nil {
		return nil, false
	}

	jsonLDScripts := make([]string, 0, 4)
	meta := map[string]string{}

	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode {
			return
		}
		tag := strings.ToLower(node.Data)
		switch tag {
		case "script":
			if !hasAttrContains(node, "type", "ld+json") {
				return
			}
			text := strings.TrimSpace(nodeText(node))
			if text != "" {
				jsonLDScripts = append(jsonLDScripts, text)
			}
		case "meta":
			key := strings.TrimSpace(getAttr(node, "property"))
			if key == "" {
				key = strings.TrimSpace(getAttr(node, "name"))
			}
			if key == "" {
				return
			}
			value := strings.TrimSpace(getAttr(node, "content"))
			if value == "" {
				return
			}
			meta[strings.ToLower(key)] = value
		}
	})

	htmlProduct := extractProductFromHTML(root, pageURL, body, meta)

	bestScore := -1
	var best map[string]any
	for _, script := range jsonLDScripts {
		items := parseJSONLD(script)
		for _, item := range items {
			if !isProductType(item["@type"]) {
				continue
			}
			candidate := buildProductFromJSONLD(item, pageURL)
			if len(candidate) == 0 {
				continue
			}
			score := scoreParsedCandidate(candidate)
			if score > bestScore {
				best = candidate
				bestScore = score
			}
		}
	}

	if len(best) > 0 {
		mergeMissingFields(best, htmlProduct)
		enrichWithMetaFields(best, meta)
		if isProductCandidate(best, meta, pageURL, body, "jsonld") {
			return sanitizeParsedData(best), true
		}
	}

	nextData := extractProductFromNextData(pageURL, body)
	if len(nextData) > 0 {
		mergeMissingFields(nextData, htmlProduct)
		enrichWithMetaFields(nextData, meta)
		if isProductCandidate(nextData, meta, pageURL, body, "next_data") {
			return sanitizeParsedData(nextData), true
		}
	}

	if len(htmlProduct) > 0 {
		enrichWithMetaFields(htmlProduct, meta)
		if isProductCandidate(htmlProduct, meta, pageURL, body, "html_product") {
			return sanitizeParsedData(htmlProduct), true
		}
	}

	og := buildProductFromOpenGraph(meta, pageURL)
	if len(og) > 0 && isProductCandidate(og, meta, pageURL, body, "opengraph") {
		enrichWithMetaFields(og, meta)
		return sanitizeParsedData(og), true
	}

	embedded := extractProductFromEmbeddedWaitingList(pageURL, body)
	if len(embedded) > 0 && isProductCandidate(embedded, meta, pageURL, body, "embedded_json") {
		enrichWithMetaFields(embedded, meta)
		return sanitizeParsedData(embedded), true
	}

	return nil, false
}

func extractProductFromNextData(pageURL string, body []byte) map[string]any {
	if len(body) == 0 {
		return nil
	}

	chunk := string(body[:minInt(len(body), 3*1024*1024)])
	matches := nextDataScriptPattern.FindStringSubmatch(chunk)
	if len(matches) < 2 {
		return nil
	}

	rawJSON := strings.TrimSpace(matches[1])
	if rawJSON == "" {
		return nil
	}

	decoder := json.NewDecoder(strings.NewReader(rawJSON))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil
	}

	pageProps := toMap(toMap(payload["props"])["pageProps"])
	if len(pageProps) == 0 {
		return nil
	}

	product := pickNextDataProduct(pageProps)
	if len(product) == 0 {
		return nil
	}

	title := cleanText(firstNonEmpty(toString(product["name"]), toString(product["title"])))
	if title == "" {
		return nil
	}

	sourceURL := firstNonEmpty(resolveURLAgainstBase(pageURL, toString(pageProps["currentUrl"])), pageURL)
	data := map[string]any{
		"title":      title,
		"source_url": sourceURL,
		"extraction": "next_data",
	}

	if description := extractNextDataDescription(product["description"]); description != "" {
		data["description"] = description
	}

	if price := firstFloatValue(
		product["bookPrice"],
		product["price"],
		product["amount"],
		product["ebookPrice"],
		product["audioPrice"],
	); price != nil {
		data["price"] = *price
	}

	if isbn := normalizeISBN(firstNonEmpty(toString(product["isbn"]), toString(product["barcode"]))); isbn != "" {
		data["isbn"] = isbn
	}
	if barcode := cleanText(toString(product["barcode"])); barcode != "" {
		data["barcode"] = barcode
	}

	if productID := cleanText(firstNonEmpty(toString(product["_id"]), toString(product["id"]))); productID != "" {
		data["product_id"] = productID
		data["sku"] = productID
	}
	if sku := cleanText(toString(product["sku"])); sku != "" {
		data["sku"] = sku
	}

	publisher := toMap(product["publisher"])
	if len(publisher) > 0 {
		if value := cleanText(firstNonEmpty(toString(publisher["name"]), toString(publisher["title"]))); value != "" {
			data["publisher"] = value
		}
	} else if value := cleanText(toString(product["publisher"])); value != "" {
		data["publisher"] = value
	}

	if authors := extractNextDataNames(firstNonEmptyAny(product["authors"], product["author"])); len(authors) > 0 {
		data["author_names"] = authors
	}
	if genres := extractNextDataNames(product["genres"]); len(genres) > 0 {
		data["genres"] = genres
	}
	if tags := extractNextDataNames(product["tags"]); len(tags) > 0 {
		data["tags"] = tags
	}

	imageRaw := firstNonEmpty(
		toString(product["imgUrl"]),
		toString(product["image"]),
		toString(pageProps["seoImage"]),
	)
	if image := normalizeNextDataImage(sourceURL, imageRaw); image != "" {
		data["image"] = image
	}

	if available, ok := product["isAvailable"].(bool); ok {
		if available {
			data["availability"] = "in_stock"
		} else {
			data["availability"] = "out_of_stock"
		}
	} else if amount := toInt(product["amount"]); amount != nil {
		if *amount > 0 {
			data["availability"] = "in_stock"
		} else {
			data["availability"] = "out_of_stock"
		}
	}

	for _, key := range []string{
		"link",
		"year",
		"numberOfPage",
		"paperFormat",
		"language",
		"cover",
		"state",
		"rating",
		"rateCount",
		"viewsCount",
		"stockCount",
		"availableCount",
		"bookPrice",
		"ebookPrice",
		"audioPrice",
		"potcastPrice",
	} {
		value := product[key]
		if isEmptyValue(value) {
			continue
		}
		data[key] = value
	}

	if !hasAnyProductSignals(data) {
		return nil
	}

	return data
}

func pickNextDataProduct(pageProps map[string]any) map[string]any {
	candidates := []any{
		pageProps["data"],
		toMap(pageProps["data"])["data"],
		pageProps["product"],
		pageProps["book"],
		pageProps["item"],
	}

	for _, raw := range candidates {
		candidate := toMap(raw)
		if looksLikeNextDataProduct(candidate) {
			return candidate
		}
	}

	return nil
}

func looksLikeNextDataProduct(candidate map[string]any) bool {
	if len(candidate) == 0 {
		return false
	}
	hasName := cleanText(firstNonEmpty(toString(candidate["name"]), toString(candidate["title"]))) != ""
	if !hasName {
		return false
	}
	for _, key := range []string{"bookPrice", "price", "barcode", "isbn", "link", "_id", "id"} {
		if !isEmptyValue(candidate[key]) {
			return true
		}
	}
	return false
}

func extractNextDataDescription(value any) string {
	switch typed := value.(type) {
	case string:
		return cleanText(typed)
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			text := ""
			switch nested := item.(type) {
			case string:
				text = cleanText(nested)
			case map[string]any:
				text = cleanText(firstNonEmpty(toString(nested["value"]), toString(nested["text"])))
			}
			if text != "" {
				parts = append(parts, text)
			}
		}
		return cleanText(strings.Join(parts, " "))
	case map[string]any:
		return cleanText(firstNonEmpty(toString(typed["value"]), toString(typed["text"])))
	default:
		return cleanText(toString(value))
	}
}

func extractNextDataNames(value any) []string {
	names := []string{}
	seen := map[string]struct{}{}
	add := func(item string) {
		item = cleanText(item)
		if item == "" {
			return
		}
		key := strings.ToLower(item)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		names = append(names, item)
	}

	switch typed := value.(type) {
	case string:
		add(typed)
	case []string:
		for _, item := range typed {
			add(item)
		}
	case []any:
		for _, item := range typed {
			switch nested := item.(type) {
			case string:
				add(nested)
			case map[string]any:
				add(firstNonEmpty(toString(nested["name"]), toString(nested["fullName"]), toString(nested["title"])))
			}
		}
	case map[string]any:
		add(firstNonEmpty(toString(typed["name"]), toString(typed["fullName"]), toString(typed["title"])))
	}

	if len(names) == 0 {
		return nil
	}
	return names
}

func firstFloatValue(values ...any) *float64 {
	for _, value := range values {
		if parsed := toFloat(value); parsed != nil {
			return parsed
		}
	}
	return nil
}

func normalizeNextDataImage(pageURL, raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") || strings.HasPrefix(lower, "data:") {
		return raw
	}

	host := ""
	if parsed, err := url.Parse(pageURL); err == nil {
		host = normalizeHost(parsed.Host)
	}
	if host == "book.uz" {
		cleaned := strings.TrimPrefix(raw, "/")
		if cleaned != "" {
			return "https://backend.book.uz/user-api/" + cleaned
		}
	}

	return resolveURLAgainstBase(pageURL, raw)
}

func parseJSONLD(raw string) []map[string]any {
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	var payload any
	if err := decoder.Decode(&payload); err != nil {
		return nil
	}
	return normalizeJSONLD(payload)
}

func normalizeJSONLD(value any) []map[string]any {
	results := []map[string]any{}
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			results = append(results, normalizeJSONLD(item)...)
		}
	case map[string]any:
		if graph, ok := typed["@graph"].([]any); ok {
			for _, item := range graph {
				results = append(results, normalizeJSONLD(item)...)
			}
		}
		results = append(results, typed)
	}
	return results
}

func isProductType(value any) bool {
	switch typed := value.(type) {
	case string:
		lower := strings.ToLower(strings.TrimSpace(typed))
		return lower == "product" || lower == "book"
	case []any:
		for _, item := range typed {
			if isProductType(item) {
				return true
			}
		}
	}
	return false
}

func buildProductFromJSONLD(item map[string]any, pageURL string) map[string]any {
	name := toString(item["name"])
	if name == "" {
		return nil
	}

	data := map[string]any{
		"title":      name,
		"source_url": firstNonEmpty(toString(item["url"]), pageURL),
		"extraction": "jsonld",
	}

	if value := toString(item["description"]); value != "" {
		data["description"] = value
	}
	if value := toImage(item["image"]); value != "" {
		data["image"] = value
	}
	if value := toString(item["sku"]); value != "" {
		data["sku"] = value
	}
	if value := firstNonEmpty(toString(item["isbn"]), toString(item["isbn13"])); value != "" {
		data["isbn"] = value
	}
	if value := firstNonEmpty(
		toString(item["gtin"]),
		toString(item["gtin13"]),
		toString(item["gtin14"]),
		toString(item["gtin12"]),
	); value != "" {
		data["gtin"] = value
	}
	if value := toString(item["brand"]); value != "" {
		data["brand"] = value
	}

	offer := toMap(item["offers"])
	if len(offer) > 0 {
		if value := toFloat(offer["price"]); value != nil {
			data["price"] = *value
		}
		if value := toString(offer["priceCurrency"]); value != "" {
			data["currency"] = value
		}
		if value := toString(offer["availability"]); value != "" {
			data["availability"] = value
		}
	}

	rating := toMap(item["aggregateRating"])
	if len(rating) > 0 {
		if value := toFloat(rating["ratingValue"]); value != nil {
			data["rating"] = *value
		}
		if value := toInt(rating["reviewCount"]); value != nil {
			data["review_count"] = *value
		}
	}

	flattened := map[string]any{}
	flattenMap(item, "", 3, flattened)
	for key, value := range flattened {
		if _, exists := data[key]; exists {
			continue
		}
		data[key] = value
	}

	return data
}

func buildProductFromOpenGraph(meta map[string]string, pageURL string) map[string]any {
	title := firstNonEmpty(meta["og:title"], meta["title"])
	if title == "" {
		return nil
	}

	data := map[string]any{
		"title":      title,
		"source_url": pageURL,
		"extraction": "opengraph",
	}
	if value := firstNonEmpty(meta["og:description"], meta["description"]); value != "" {
		data["description"] = value
	}
	if value := meta["og:image"]; value != "" {
		data["image"] = value
	}
	if price := toFloat(meta["product:price:amount"]); price != nil {
		data["price"] = *price
	}
	if value := meta["product:price:currency"]; value != "" {
		data["currency"] = value
	}
	if value := meta["product:availability"]; value != "" {
		data["availability"] = value
	}
	return data
}

func extractProductFromHTML(root *xhtml.Node, pageURL string, body []byte, meta map[string]string) map[string]any {
	if root == nil {
		return nil
	}

	data := map[string]any{
		"source_url": pageURL,
		"extraction": "html_product",
	}

	// H1 title from product templates (WooCommerce and custom themes).
	if title := findBestHTMLTitle(root); title != "" {
		data["title"] = title
		if author, pureTitle := splitCompositeTitle(title); author != "" {
			data["author_names"] = []string{author}
			if pureTitle != "" {
				data["title"] = pureTitle
			}
		}
	}
	if toString(data["title"]) == "" {
		if fallback := firstNonEmpty(meta["og:title"], meta["twitter:title"], meta["title"]); fallback != "" {
			data["title"] = fallback
		}
	}

	// Description blocks.
	if description := findBestDescription(root); description != "" {
		data["description"] = description
	}
	if toString(data["description"]) == "" {
		if fallback := firstNonEmpty(meta["og:description"], meta["description"], meta["twitter:description"]); fallback != "" {
			data["description"] = fallback
		}
	}

	// Primary product image.
	if imageURL := findBestImage(root); imageURL != "" {
		data["image"] = imageURL
	}
	if toString(data["image"]) == "" {
		if fallback := firstNonEmpty(meta["og:image"], meta["twitter:image"]); fallback != "" {
			data["image"] = fallback
		}
	}

	// Read "param-name / param-value" rows that many stores use for product attributes.
	attrs := map[string]string{}
	attrRows := extractAttributeRows(root)
	for _, row := range attrRows {
		label := normalizeLabel(row.name)
		value := strings.TrimSpace(row.value)
		if label == "" || value == "" {
			continue
		}
		attrs[label] = value
		attrKey := "attr." + slugifyLabel(label)
		data[attrKey] = value
		applyAttrToProductData(data, label, value)
	}
	if isbn := normalizeISBN(toString(data["isbn"])); isbn != "" {
		data["isbn"] = isbn
	}
	if toString(data["isbn"]) == "" {
		if isbn := findFirstAttrValue(root, "data-product-isbn"); isbn != "" {
			if normalized := normalizeISBN(isbn); normalized != "" {
				data["isbn"] = normalized
			}
		}
	}
	if toString(data["sku"]) == "" {
		if sku := findFirstAttrValue(root, "data-product-id"); sku != "" {
			sku = strings.TrimSpace(sku)
			if numericIDPattern.MatchString(sku) {
				data["sku"] = sku
			}
		}
	}

	// Availability from page status labels.
	availability := detectAvailability(root)
	if availability != "" {
		data["availability"] = availability
	}

	// Age marker outside attribute table.
	if age := detectAgeRestriction(root); age != "" {
		if toString(data["age_restriction"]) == "" {
			data["age_restriction"] = age
		}
	}

	// Breadcrumb category (useful for mapping).
	if category := detectBreadcrumbCategory(root); category != "" {
		data["category"] = category
	}

	// Parse ecommerce JS payload when available (id/name/price/category/currency).
	if info := extractEcommerceInfo(body); len(info) > 0 {
		mergeMissingFields(data, info)
	}
	if _, exists := data["price"]; !exists {
		if price := toFloat(meta["product:price:amount"]); price != nil {
			data["price"] = *price
		}
	}
	if toString(data["currency"]) == "" && strings.TrimSpace(meta["product:price:currency"]) != "" {
		data["currency"] = strings.TrimSpace(meta["product:price:currency"])
	}

	if !hasAnyProductSignals(data) {
		return nil
	}
	return data
}

type attrRow struct {
	name  string
	value string
}

func extractAttributeRows(root *xhtml.Node) []attrRow {
	rows := []attrRow{}
	seen := map[string]struct{}{}
	addRow := func(name, value string) {
		name = cleanText(name)
		value = cleanText(value)
		if name == "" || value == "" {
			return
		}
		key := strings.ToLower(name) + "::" + strings.ToLower(value)
		if _, exists := seen[key]; exists {
			return
		}
		seen[key] = struct{}{}
		rows = append(rows, attrRow{name: name, value: value})
	}

	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode {
			return
		}
		if hasClassContains(node, "params-row") {
			addRow(
				nodeText(findFirstByClass(node, "param-name")),
				nodeText(findFirstByClass(node, "param-value")),
			)
		}

		isInAttributeScope := func(target *xhtml.Node) bool {
			for parent := target.Parent; parent != nil; parent = parent.Parent {
				if parent.Type != xhtml.ElementNode {
					continue
				}
				class := strings.ToLower(getAttr(parent, "class"))
				id := strings.ToLower(getAttr(parent, "id"))
				if strings.Contains(class, "attribute") ||
					strings.Contains(class, "product") ||
					strings.Contains(class, "data") ||
					strings.Contains(class, "params") ||
					strings.Contains(class, "spec") ||
					strings.Contains(id, "attribute") ||
					strings.Contains(id, "tab-attribute") {
					return true
				}
			}
			return false
		}

		if strings.EqualFold(node.Data, "tr") {
			if !isInAttributeScope(node) {
				return
			}
			cells := make([]*xhtml.Node, 0, 3)
			for child := node.FirstChild; child != nil; child = child.NextSibling {
				if child.Type != xhtml.ElementNode {
					continue
				}
				if strings.EqualFold(child.Data, "td") || strings.EqualFold(child.Data, "th") {
					cells = append(cells, child)
				}
			}
			if len(cells) < 2 {
				return
			}
			addRow(nodeText(cells[0]), nodeText(cells[1]))
			return
		}

		if strings.EqualFold(node.Data, "li") && isInAttributeScope(node) {
			text := cleanText(nodeText(node))
			if !strings.Contains(text, ":") {
				return
			}
			parts := strings.SplitN(text, ":", 2)
			if len(parts) != 2 {
				return
			}
			addRow(parts[0], parts[1])
			return
		}

		if (strings.EqualFold(node.Data, "p") || strings.EqualFold(node.Data, "div")) && isInAttributeScope(node) {
			text := cleanText(nodeText(node))
			if !strings.Contains(text, ":") {
				return
			}
			for _, pair := range extractInlineAttrPairs(text) {
				addRow(pair.name, pair.value)
			}
		}
	})
	return rows
}

func extractInlineAttrPairs(text string) []attrRow {
	text = cleanText(text)
	if text == "" {
		return nil
	}
	keyIndexes := inlineAttrKeyPattern.FindAllStringIndex(text, -1)
	if len(keyIndexes) == 0 {
		return nil
	}
	rows := make([]attrRow, 0, len(keyIndexes))
	for idx, keyRange := range keyIndexes {
		if len(keyRange) < 2 || keyRange[1] <= keyRange[0] {
			continue
		}
		name := strings.TrimSpace(strings.TrimSuffix(text[keyRange[0]:keyRange[1]], ":"))
		valueStart := keyRange[1]
		valueEnd := len(text)
		if idx+1 < len(keyIndexes) && len(keyIndexes[idx+1]) == 2 {
			valueEnd = keyIndexes[idx+1][0]
		}
		value := cleanText(text[valueStart:valueEnd])
		value = strings.Trim(value, " ;,.-")
		if name == "" || value == "" {
			continue
		}
		rows = append(rows, attrRow{name: name, value: value})
	}
	return rows
}

func applyAttrToProductData(data map[string]any, label string, value string) {
	low := strings.ToLower(strings.TrimSpace(label))
	switch {
	case strings.Contains(low, "isbn"):
		if normalized := normalizeISBN(value); normalized != "" {
			data["isbn"] = normalized
		} else {
			data["isbn"] = value
		}
	case strings.Contains(low, "артикул") || strings.Contains(low, "sku") || strings.Contains(low, "код"):
		cleaned := strings.TrimSpace(value)
		if cleaned == "" {
			return
		}
		if strings.Contains(strings.ToLower(cleaned), "copy") {
			return
		}
		data["sku"] = cleaned
	case strings.Contains(low, "автор") || strings.Contains(low, "author"):
		authors := splitNames(value)
		if len(authors) > 0 {
			data["author_names"] = authors
		}
	case strings.Contains(low, "страниц") || strings.Contains(low, "pages"):
		if pages := toInt(value); pages != nil {
			data["pages"] = *pages
		}
	case strings.Contains(low, "формат") || strings.Contains(low, "format"):
		data["format"] = value
	case strings.Contains(low, "облож") || strings.Contains(low, "binding"):
		data["binding_type"] = value
	case strings.Contains(low, "возраст") || strings.Contains(low, "age"):
		data["age_restriction"] = value
	case strings.Contains(low, "оригиналь") || strings.Contains(low, "original"):
		data["original_title"] = value
	case strings.Contains(low, "переводчик") || strings.Contains(low, "translator"):
		data["translator"] = value
	case strings.Contains(low, "редактор") || strings.Contains(low, "editor"):
		data["editor"] = value
	case strings.Contains(low, "художник") || strings.Contains(low, "illustrator") || strings.Contains(low, "painter"):
		data["painter"] = value
	case strings.Contains(low, "дата выхода") || strings.Contains(low, "издания") || strings.Contains(low, "publish"):
		data["publication_date"] = value
	case strings.Contains(low, "цена") || strings.Contains(low, "price"):
		if price := toFloat(value); price != nil {
			data["price"] = *price
		}
	}
}

func findBestHTMLTitle(root *xhtml.Node) string {
	var candidates []string
	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode {
			return
		}
		text := cleanText(nodeText(node))
		if text == "" {
			return
		}
		class := strings.ToLower(getAttr(node, "class"))
		if strings.Contains(class, "product-title") {
			candidates = append([]string{text}, candidates...)
			return
		}
		if !strings.EqualFold(node.Data, "h1") {
			return
		}
		if strings.Contains(class, "book-title") || strings.Contains(class, "product_title") || strings.Contains(class, "entry-title") {
			candidates = append([]string{text}, candidates...)
			return
		}
		candidates = append(candidates, text)
	})
	if len(candidates) == 0 {
		return ""
	}
	return strings.TrimSpace(candidates[0])
}

func findBestDescription(root *xhtml.Node) string {
	best := ""
	bestScore := -1
	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode {
			return
		}
		class := strings.ToLower(getAttr(node, "class"))
		id := strings.ToLower(getAttr(node, "id"))
		if !(strings.Contains(class, "description") ||
			strings.Contains(class, "entry-content") ||
			strings.Contains(class, "annotation") ||
			strings.Contains(class, "about") ||
			strings.Contains(class, "page-text") ||
			strings.Contains(class, "product-tab") ||
			strings.Contains(id, "description") ||
			strings.Contains(id, "annotation")) {
			return
		}
		text := cleanText(nodeText(node))
		if text == "" {
			return
		}
		score := len([]rune(text))
		if strings.Contains(class, "description-block") {
			score += 120
		}
		if strings.Contains(class, "tab-content") {
			score += 20
		}
		if score > bestScore {
			bestScore = score
			best = text
		}
	})
	return best
}

func findBestImage(root *xhtml.Node) string {
	best := ""
	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode || !strings.EqualFold(node.Data, "img") {
			return
		}
		if best == "" {
			best = firstNonEmpty(
				strings.TrimSpace(getAttr(node, "src")),
				strings.TrimSpace(getAttr(node, "data-src")),
			)
		}
		if strings.TrimSpace(getAttr(node, "itemprop")) == "image" {
			best = firstNonEmpty(
				strings.TrimSpace(getAttr(node, "data-src")),
				strings.TrimSpace(getAttr(node, "src")),
				best,
			)
		}
	})
	best = strings.TrimSpace(best)
	if strings.HasPrefix(best, "data:image/") {
		return ""
	}
	return best
}

func detectAvailability(root *xhtml.Node) string {
	availability := ""
	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode {
			return
		}
		class := strings.ToLower(getAttr(node, "class"))
		if class == "" {
			return
		}
		text := strings.ToLower(cleanText(nodeText(node)))
		if text == "" {
			return
		}
		if strings.Contains(class, "not-available") || strings.Contains(text, "нет в наличии") || strings.Contains(text, "out of stock") {
			availability = "out_of_stock"
			return
		}
		if availability == "" && (strings.Contains(text, "в наличии") || strings.Contains(text, "в продаже") || strings.Contains(text, "in stock")) {
			availability = "in_stock"
		}
	})
	return availability
}

func detectAgeRestriction(root *xhtml.Node) string {
	age := ""
	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode {
			return
		}
		id := strings.ToLower(strings.TrimSpace(getAttr(node, "id")))
		if id != "age_dopusk" && id != "age" {
			return
		}
		value := cleanText(nodeText(node))
		if value != "" {
			age = value
		}
	})
	return age
}

func detectBreadcrumbCategory(root *xhtml.Node) string {
	category := ""
	var breadcrumbs []string
	walkHTML(root, func(node *xhtml.Node) {
		if node.Type != xhtml.ElementNode || !strings.EqualFold(node.Data, "li") {
			return
		}
		if !hasAttrContains(node, "itemtype", "ListItem") {
			return
		}
		text := cleanText(nodeText(node))
		if text != "" {
			breadcrumbs = append(breadcrumbs, text)
		}
	})
	if len(breadcrumbs) >= 2 {
		category = breadcrumbs[len(breadcrumbs)-2]
	}
	return category
}

func extractEcommerceInfo(body []byte) map[string]any {
	text := string(body)
	startIdx := strings.Index(text, "wpym.ec.addData(")
	if startIdx < 0 {
		return nil
	}
	openIdx := strings.Index(text[startIdx:], "(")
	if openIdx < 0 {
		return nil
	}
	openIdx += startIdx

	jsonChunk, ok := extractJSONObjectFromText(text[openIdx+1:])
	if !ok || strings.TrimSpace(jsonChunk) == "" {
		return nil
	}

	decoder := json.NewDecoder(strings.NewReader(jsonChunk))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil
	}

	out := map[string]any{}
	if currency := toString(payload["currency"]); currency != "" {
		out["currency"] = currency
	}
	products, ok := payload["products"].(map[string]any)
	if !ok || len(products) == 0 {
		return out
	}
	for _, raw := range products {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if id := toString(item["id"]); id != "" {
			// On many WooCommerce stores this is ISBN or SKU.
			if strings.Contains(strings.ToLower(id), "-") && toString(out["isbn"]) == "" {
				out["isbn"] = id
			}
			if toString(out["sku"]) == "" {
				out["sku"] = id
			}
		}
		if name := toString(item["name"]); name != "" {
			out["title"] = name
		}
		if category := toString(item["category"]); category != "" {
			out["category"] = category
		}
		if price := toFloat(item["price"]); price != nil {
			out["price"] = *price
		}
		break
	}
	return out
}

func extractProductFromEmbeddedWaitingList(pageURL string, body []byte) map[string]any {
	if len(body) == 0 {
		return nil
	}

	chunk := string(body[:minInt(len(body), 3*1024*1024)])
	matches := waitingListJSONPattern.FindStringSubmatch(chunk)
	if len(matches) < 2 {
		return nil
	}

	decoder := json.NewDecoder(strings.NewReader(matches[1]))
	decoder.UseNumber()
	var items []map[string]any
	if err := decoder.Decode(&items); err != nil || len(items) == 0 {
		return nil
	}

	categoryToken := ""
	if parsedPageURL, err := url.Parse(pageURL); err == nil {
		parts := strings.Split(strings.Trim(parsedPageURL.Path, "/"), "/")
		if len(parts) >= 2 && parts[0] == "catalog" {
			categoryToken = "/" + parts[1] + "/"
		}
	}

	startIndex := 0
	for _, ch := range pageURL {
		startIndex += int(ch)
	}
	if len(items) > 0 {
		startIndex %= len(items)
	}

	build := func(item map[string]any) map[string]any {
		name := cleanText(toString(item["name"]))
		rawURL := toString(item["url"])
		if name == "" || rawURL == "" {
			return nil
		}

		sourceURL := resolveURLAgainstBase(pageURL, rawURL)
		if sourceURL == "" {
			sourceURL = pageURL
		}

		data := map[string]any{
			"title":      name,
			"source_url": sourceURL,
			"extraction": "waiting_list_json",
		}
		if price := toFloat(item["price"]); price != nil {
			data["price"] = *price
		}
		if image := resolveURLAgainstBase(pageURL, toString(item["image"])); image != "" {
			data["image"] = image
		}
		if sku := firstNonEmpty(
			cleanText(toString(item["vendorCode"])),
			cleanText(toString(item["code"])),
			cleanText(toString(item["productId"])),
		); sku != "" {
			data["sku"] = sku
		}
		switch typed := item["inStock"].(type) {
		case bool:
			if typed {
				data["availability"] = "in_stock"
			} else {
				data["availability"] = "out_of_stock"
			}
		}
		return data
	}

	if categoryToken != "" {
		for _, item := range items {
			if strings.Contains(toString(item["url"]), categoryToken) {
				if data := build(item); len(data) > 0 {
					return data
				}
			}
		}
	}

	for offset := 0; offset < len(items); offset++ {
		idx := (startIndex + offset) % len(items)
		if data := build(items[idx]); len(data) > 0 {
			return data
		}
	}

	return nil
}

func resolveURLAgainstBase(baseURL, raw string) string {
	raw = strings.TrimSpace(strings.ReplaceAll(raw, `\/`, "/"))
	if raw == "" {
		return ""
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	resolved := base.ResolveReference(parsed)
	if resolved == nil || (resolved.Scheme != "http" && resolved.Scheme != "https") {
		return ""
	}
	resolved.Fragment = ""
	return resolved.String()
}

func extractJSONObjectFromText(text string) (string, bool) {
	text = strings.TrimSpace(text)
	start := strings.Index(text, "{")
	if start < 0 {
		return "", false
	}
	depth := 0
	inString := false
	escape := false
	for idx := start; idx < len(text); idx++ {
		ch := text[idx]
		if inString {
			if escape {
				escape = false
				continue
			}
			if ch == '\\' {
				escape = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return text[start : idx+1], true
			}
		}
	}
	return "", false
}

func hasAnyProductSignals(data map[string]any) bool {
	if len(data) == 0 {
		return false
	}
	for _, key := range []string{
		"title", "isbn", "sku", "price", "pages", "author_names", "format", "binding_type", "age_restriction",
	} {
		if !isEmptyValue(data[key]) {
			return true
		}
	}
	return false
}

func mergeMissingFields(dst map[string]any, src map[string]any) {
	if len(dst) == 0 || len(src) == 0 {
		return
	}
	for key, value := range src {
		if key == "" || value == nil {
			continue
		}
		if current, exists := dst[key]; !exists || isEmptyValue(current) {
			dst[key] = value
		}
	}
}

func isEmptyValue(value any) bool {
	if value == nil {
		return true
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed) == ""
	case []string:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	}
	return strings.TrimSpace(toString(value)) == ""
}

func normalizeLabel(value string) string {
	decoded := stdhtml.UnescapeString(value)
	decoded = cleanText(decoded)
	return strings.TrimSpace(decoded)
}

func slugifyLabel(value string) string {
	value = strings.ToLower(normalizeLabel(value))
	builder := strings.Builder{}
	lastUnderscore := false
	for _, r := range value {
		isAllowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || (r >= 'а' && r <= 'я') || r == 'ё'
		if isAllowed {
			builder.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			builder.WriteByte('_')
			lastUnderscore = true
		}
	}
	result := strings.Trim(builder.String(), "_")
	if result == "" {
		return "field"
	}
	return result
}

func cleanText(value string) string {
	value = stdhtml.UnescapeString(value)
	value = strings.ReplaceAll(value, "\u00a0", " ")
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "\t", " ")
	value = strings.Join(strings.Fields(value), " ")
	return strings.TrimSpace(value)
}

func normalizeISBN(value string) string {
	value = strings.ToUpper(cleanText(value))
	if value == "" {
		return ""
	}
	match := isbnValuePattern.FindString(value)
	match = strings.Trim(match, " -")
	if match == "" {
		return ""
	}
	digits := 0
	for _, ch := range match {
		if ch >= '0' && ch <= '9' {
			digits++
		}
	}
	if digits < 9 {
		return ""
	}
	return match
}

func sanitizeParsedData(data map[string]any) map[string]any {
	if len(data) == 0 {
		return data
	}
	sanitized := make(map[string]any, len(data))
	for key, value := range data {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		sanitized[key] = sanitizeParsedValue(value)
	}
	return sanitized
}

func sanitizeParsedValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return sanitizeParsedString(typed)
	case []string:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			cleaned := sanitizeParsedString(item)
			if cleaned != "" {
				result = append(result, cleaned)
			}
		}
		return result
	case []any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, sanitizeParsedValue(item))
		}
		return result
	case map[string]any:
		return sanitizeParsedData(typed)
	default:
		return value
	}
}

func sanitizeParsedString(value string) string {
	if value == "" {
		return ""
	}
	value = strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case r < 32 || r == 127:
			return -1
		default:
			return r
		}
	}, value)
	value = strings.ReplaceAll(value, "\u00a0", " ")
	value = strings.Join(strings.Fields(value), " ")
	return strings.TrimSpace(value)
}

func hasClassContains(node *xhtml.Node, token string) bool {
	class := strings.ToLower(getAttr(node, "class"))
	token = strings.ToLower(strings.TrimSpace(token))
	return token != "" && strings.Contains(class, token)
}

func findFirstByClass(root *xhtml.Node, classToken string) *xhtml.Node {
	if root == nil || strings.TrimSpace(classToken) == "" {
		return nil
	}
	var result *xhtml.Node
	walkHTML(root, func(node *xhtml.Node) {
		if result != nil {
			return
		}
		if node.Type != xhtml.ElementNode {
			return
		}
		if hasClassContains(node, classToken) {
			result = node
		}
	})
	return result
}

func splitCompositeTitle(value string) (author string, title string) {
	value = cleanText(value)
	if value == "" {
		return "", ""
	}
	parts := strings.Split(value, "|")
	if len(parts) < 2 {
		return "", value
	}
	left := strings.TrimSpace(parts[0])
	right := strings.TrimSpace(strings.Join(parts[1:], "|"))
	if left == "" || right == "" {
		return "", value
	}
	return left, right
}

func splitNames(value string) []string {
	value = cleanText(value)
	if value == "" {
		return nil
	}
	for _, sep := range []string{",", ";", "|", "/"} {
		if strings.Contains(value, sep) {
			parts := strings.Split(value, sep)
			result := make([]string, 0, len(parts))
			for _, item := range parts {
				item = strings.TrimSpace(item)
				if item != "" {
					result = append(result, item)
				}
			}
			if len(result) > 0 {
				return result
			}
		}
	}
	return []string{value}
}

func enrichWithMetaFields(data map[string]any, meta map[string]string) {
	if len(data) == 0 || len(meta) == 0 {
		return
	}
	for key, value := range meta {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		outKey := "meta." + key
		if _, exists := data[outKey]; exists {
			continue
		}
		data[outKey] = value
	}
}

func isProductCandidate(
	data map[string]any,
	meta map[string]string,
	pageURL string,
	body []byte,
	source string,
) bool {
	if len(data) == 0 {
		return false
	}
	title := strings.ToLower(strings.TrimSpace(toString(data["title"])))
	urlLower := strings.ToLower(strings.TrimSpace(pageURL))

	strongID := firstNonEmpty(
		toString(data["gtin"]),
		toString(data["isbn"]),
		toString(data["sku"]),
	) != ""
	hasPrice := toFloat(data["price"]) != nil
	hasAvailability := strings.TrimSpace(toString(data["availability"])) != ""
	hasDescription := len([]rune(strings.TrimSpace(toString(data["description"])))) >= 60
	hasImage := strings.TrimSpace(toString(data["image"])) != ""
	urlProductLike := looksLikeProductURL(urlLower)
	metaProduct := hasProductMeta(meta)
	bodyProduct := hasProductMarkersInBody(body)

	if isExplicitNonProductPage(urlLower, title) && !(strongID || hasPrice || metaProduct || bodyProduct) {
		return false
	}

	score := 0
	if strongID {
		score += 4
	}
	if hasPrice {
		score += 3
	}
	if hasAvailability {
		score++
	}
	if hasDescription {
		score++
	}
	if hasImage {
		score++
	}
	if urlProductLike {
		score += 2
	}
	if metaProduct {
		score += 2
	}
	if bodyProduct {
		score += 2
	}

	if source == "jsonld" {
		// JSON-LD Product/Book can appear on listing/info pages, so keep IDs strict.
		if hasPrice || metaProduct {
			return true
		}
		if strongID && (urlProductLike || hasAvailability) {
			return true
		}
		return score >= 6
	}
	if source == "html_product" {
		if isExplicitNonProductPage(urlLower, title) {
			return strongID || hasPrice || metaProduct
		}
		if strongID || hasPrice || metaProduct || bodyProduct {
			return score >= 4
		}
		return title != "" && (hasDescription || hasImage) && isLikelyProductPath(urlLower)
	}

	// OpenGraph is weak for catalogs/category pages, so keep it strict.
	if !(strongID || hasPrice || metaProduct || bodyProduct) {
		return false
	}
	return score >= 6
}

func hasProductMeta(meta map[string]string) bool {
	if len(meta) == 0 {
		return false
	}
	for _, key := range []string{
		"product:price:amount",
		"product:price:currency",
		"product:availability",
		"product:retailer_item_id",
		"og:type",
	} {
		value := strings.ToLower(strings.TrimSpace(meta[key]))
		if value == "" {
			continue
		}
		if key == "og:type" {
			if strings.Contains(value, "product") || strings.Contains(value, "book") {
				return true
			}
			continue
		}
		return true
	}
	return false
}

func hasProductMarkersInBody(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	peekSize := minInt(len(body), 120000)
	chunk := strings.ToLower(string(body[:peekSize]))
	for _, marker := range []string{
		`"@type":"product"`,
		`"@type": "product"`,
		"schema.org/product",
		"product:price:amount",
		"itemprop=\"sku\"",
		"itemprop=\"isbn\"",
		"itemprop=\"gtin\"",
		"product-detail-page__title",
		"product-detail__title",
		`class="product-info"`,
		`id="tab-attribute"`,
		"route=product/product",
		"product_id=",
	} {
		if strings.Contains(chunk, marker) {
			return true
		}
	}
	return false
}

func looksLikeProductURL(urlLower string) bool {
	if strings.Contains(urlLower, "/catalog/product/") || catalogProductSlugPattern.MatchString(urlLower) || booksDeepProductPathPattern.MatchString(urlLower) || catalogDeepProductPathPattern.MatchString(urlLower) {
		return true
	}
	for _, token := range []string{
		"/product/",
		"/products/",
		"/book/",
		"/books/",
		"/item/",
		"/isbn/",
		"/p/",
		"/dp/",
		"sku",
		"isbn",
		"gtin",
		"route=product/product",
		"product_id=",
	} {
		if strings.Contains(urlLower, token) {
			return true
		}
	}
	return false
}

func isExplicitNonProductPage(urlLower, titleLower string) bool {
	if isLikelyProductPath(urlLower) {
		return false
	}
	if paginationPathPattern.MatchString(urlLower) {
		return true
	}
	blockers := []string{
		"/catalog/",
		"/catalogue/",
		"catalog?",
		"catalog/",
		"catalogue?",
		"catalogue/",
		"/category/",
		"/categories/",
		"/collections/",
		"/collection/",
		"/bestseller",
		"/novinki",
		"/new/",
		"/news/",
		"/authors/",
		"/author/",
		"/gallery/",
		"/galleries/",
		"/blog/",
		"/articles/",
		"/article/",
		"/search",
		"/refund",
		"/delivery",
		"/payment",
		"/contacts",
		"/about",
		"/subscription",
		"/certificates",
		"/audiobookcatalog",
		"/ebookcatalog",
		"/cart",
		"/basket",
		"/wishlist",
	}
	for _, token := range blockers {
		if strings.Contains(urlLower, token) {
			// Allow obvious product path patterns to override generic catalog tokens.
			if strings.Contains(urlLower, "/product/") || strings.Contains(urlLower, "/book/") {
				return false
			}
			return true
		}
	}
	for _, token := range []string{
		"каталог",
		"бестселлер",
		"новинки",
		"книги -",
		"catalog",
		"category",
		"categories",
		"collection",
	} {
		if strings.Contains(titleLower, token) {
			return true
		}
	}
	return false
}

func scoreParsedCandidate(data map[string]any) int {
	score := 0
	if toString(data["title"]) != "" {
		score += 2
	}
	if _, ok := data["price"]; ok {
		score += 2
	}
	if toString(data["description"]) != "" {
		score++
	}
	if toString(data["image"]) != "" {
		score++
	}
	if toString(data["gtin"]) != "" || toString(data["isbn"]) != "" || toString(data["sku"]) != "" {
		score += 2
	}
	return score
}

func walkHTML(node *xhtml.Node, visit func(*xhtml.Node)) {
	if node == nil {
		return
	}
	visit(node)
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		walkHTML(child, visit)
	}
}

func nodeText(node *xhtml.Node) string {
	if node == nil {
		return ""
	}
	builder := strings.Builder{}
	var visit func(*xhtml.Node)
	visit = func(current *xhtml.Node) {
		if current.Type == xhtml.TextNode {
			builder.WriteString(current.Data)
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			visit(child)
		}
	}
	visit(node)
	return builder.String()
}

func getAttr(node *xhtml.Node, key string) string {
	for _, attr := range node.Attr {
		if strings.EqualFold(attr.Key, key) {
			return attr.Val
		}
	}
	return ""
}

func hasAttrContains(node *xhtml.Node, key, token string) bool {
	value := strings.ToLower(getAttr(node, key))
	token = strings.ToLower(token)
	return strings.Contains(value, token)
}

func findFirstAttrValue(root *xhtml.Node, key string) string {
	if root == nil || strings.TrimSpace(key) == "" {
		return ""
	}
	key = strings.ToLower(strings.TrimSpace(key))
	value := ""
	walkHTML(root, func(node *xhtml.Node) {
		if value != "" || node.Type != xhtml.ElementNode {
			return
		}
		for _, attr := range node.Attr {
			if strings.EqualFold(strings.TrimSpace(attr.Key), key) {
				trimmed := strings.TrimSpace(attr.Val)
				if trimmed != "" {
					value = trimmed
					return
				}
			}
		}
	})
	return value
}

func toMap(value any) map[string]any {
	switch typed := value.(type) {
	case map[string]any:
		return typed
	case []any:
		for _, item := range typed {
			if asMap, ok := item.(map[string]any); ok {
				return asMap
			}
		}
	}
	return nil
}

func toString(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return strings.TrimSpace(typed.String())
	case float64:
		if math.Mod(typed, 1) == 0 {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(typed), 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	case map[string]any:
		return firstNonEmpty(toString(typed["name"]), toString(typed["@id"]))
	case []any:
		parts := make([]string, 0, len(typed))
		for _, item := range typed {
			text := toString(item)
			if text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) == 0 {
			return ""
		}
		return strings.Join(parts, ", ")
	default:
		return ""
	}
}

func toImage(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		return toString(typed["url"])
	case []any:
		for _, item := range typed {
			if image := toImage(item); image != "" {
				return image
			}
		}
	}
	return ""
}

func toFloat(value any) *float64 {
	switch typed := value.(type) {
	case nil:
		return nil
	case float64:
		v := typed
		return &v
	case float32:
		v := float64(typed)
		return &v
	case int:
		v := float64(typed)
		return &v
	case int64:
		v := float64(typed)
		return &v
	case json.Number:
		parsed, err := typed.Float64()
		if err != nil {
			return nil
		}
		return &parsed
	case string:
		normalized := strings.TrimSpace(strings.ReplaceAll(typed, ",", "."))
		if normalized == "" {
			return nil
		}
		parsed, err := strconv.ParseFloat(normalized, 64)
		if err != nil {
			return nil
		}
		return &parsed
	}
	return nil
}

func toInt(value any) *int {
	switch typed := value.(type) {
	case nil:
		return nil
	case int:
		v := typed
		return &v
	case int64:
		v := int(typed)
		return &v
	case float64:
		v := int(typed)
		return &v
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			floatParsed, ferr := typed.Float64()
			if ferr != nil {
				return nil
			}
			v := int(floatParsed)
			return &v
		}
		v := int(parsed)
		return &v
	case string:
		digits := strings.Builder{}
		for _, ch := range typed {
			if ch >= '0' && ch <= '9' {
				digits.WriteRune(ch)
			}
		}
		if digits.Len() == 0 {
			return nil
		}
		parsed, err := strconv.Atoi(digits.String())
		if err != nil {
			return nil
		}
		return &parsed
	}
	return nil
}

func flattenMap(value any, prefix string, depth int, out map[string]any) {
	if depth < 0 || out == nil {
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, item := range typed {
			key = strings.TrimSpace(key)
			if key == "" {
				continue
			}
			nextPrefix := key
			if prefix != "" {
				nextPrefix = prefix + "." + key
			}
			flattenMap(item, nextPrefix, depth-1, out)
		}
	case []any:
		if len(typed) == 0 {
			return
		}
		if allPrimitive(typed) {
			values := make([]string, 0, len(typed))
			for _, item := range typed {
				text := toString(item)
				if text != "" {
					values = append(values, text)
				}
			}
			if len(values) > 0 && prefix != "" {
				out[prefix] = strings.Join(values, ", ")
			}
			return
		}
		if depth <= 0 {
			return
		}
		for index, item := range typed {
			nextPrefix := fmt.Sprintf("%s.%d", prefix, index)
			flattenMap(item, nextPrefix, depth-1, out)
		}
	default:
		if prefix != "" {
			out[prefix] = typed
		}
	}
}

func allPrimitive(values []any) bool {
	for _, item := range values {
		switch item.(type) {
		case string, float64, float32, int, int64, int32, bool, json.Number:
			continue
		default:
			return false
		}
	}
	return true
}

func collectDetectedFields(records []models.ParserRecord) []string {
	fieldSet := map[string]struct{}{}
	for _, record := range records {
		for key := range record.Data {
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
	return fields
}

func collectDetectedFieldsFromSet(fieldSet map[string]struct{}) []string {
	if len(fieldSet) == 0 {
		return nil
	}
	fields := make([]string, 0, len(fieldSet))
	for key := range fieldSet {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		fields = append(fields, key)
	}
	sort.Strings(fields)
	return fields
}

func applyRules(data map[string]any, rules map[string]models.ParserFieldRule, prefix string) map[string]any {
	mapped := map[string]any{}
	for target, rule := range rules {
		if !strings.HasPrefix(target, prefix) {
			continue
		}
		field := strings.TrimSpace(strings.TrimPrefix(target, prefix))
		if field == "" {
			continue
		}
		if strings.TrimSpace(rule.Constant) != "" {
			mapped[field] = strings.TrimSpace(rule.Constant)
			continue
		}
		sourceField := strings.TrimSpace(rule.Source)
		if sourceField == "" {
			continue
		}
		if value, ok := data[sourceField]; ok {
			mapped[field] = value
		}
	}
	return mapped
}

func buildEksmoProductFromMapping(mapped map[string]any, source map[string]any, runID primitive.ObjectID) (models.EksmoProduct, bool) {
	now := time.Now().UTC()

	nomcode := firstNonEmpty(toString(mapped["nomcode"]), toString(mapped["barcode"]))
	guidNom := toString(mapped["guidNom"])
	guid := toString(mapped["guid"])
	if guidNom == "" && guid == "" && nomcode == "" {
		guidNom = syntheticID("eksmo", firstNonEmpty(toString(source["source_url"]), toString(source["title"]), toString(source["isbn"]), toString(source["gtin"])))
	}

	product := models.EksmoProduct{
		GUIDNOM: guidNom,
		GUID:    guid,
		NomCode: nomcode,
		ISBN:    firstNonEmpty(toString(mapped["isbn"]), toString(source["isbn"]), toString(source["gtin"])),
		Name:    firstNonEmpty(toString(mapped["name"]), toString(source["title"])),

		AuthorCover:    toString(mapped["authorCover"]),
		Annotation:     firstNonEmpty(toString(mapped["annotation"]), toString(source["description"])),
		CoverURL:       firstNonEmpty(toString(mapped["coverUrl"]), toString(source["image"])),
		AgeRestriction: toString(mapped["ageRestriction"]),
		Format:         toString(mapped["format"]),
		PaperType:      toString(mapped["paperType"]),
		BindingType:    toString(mapped["bindingType"]),
		SubjectName:    toString(mapped["subjectName"]),
		BrandName:      toString(mapped["brandName"]),
		SerieName:      toString(mapped["serieName"]),
		PublisherName:  toString(mapped["publisher"]),
		AuthorNames:    toStringSlice(mapped["authorNames"]),
		TagNames:       toStringSlice(mapped["tagNames"]),
		GenreNames:     toStringSlice(mapped["genreNames"]),
		SyncedAt:       now,
		UpdatedAt:      now,
		Raw: bson.M{
			"parserRunId": runID.Hex(),
			"sourceUrl":   toString(source["source_url"]),
			"parserData":  source,
		},
	}

	if pages := toInt(mapped["pages"]); pages != nil {
		product.Pages = *pages
	}
	if price := toFloat(firstNonEmptyAny(mapped["price"], source["price"])); price != nil {
		product.Raw["PRICE"] = *price
	}

	if strings.TrimSpace(product.Name) == "" {
		return models.EksmoProduct{}, false
	}
	return product, true
}

func buildMainProductFromMapping(mapped map[string]any, source map[string]any, fallback models.EksmoProduct) (models.MainProduct, bool) {
	now := time.Now().UTC()
	path := parseCategoryPath(mapped["categoryPath"])
	fallbackNicheName := ""
	if fallback.Niche != nil {
		fallbackNicheName = fallback.Niche.Name
	}

	main := models.MainProduct{
		SourceGUIDNOM: firstNonEmpty(toString(mapped["sourceGuidNom"]), fallback.GUIDNOM),
		SourceGUID:    firstNonEmpty(toString(mapped["sourceGuid"]), fallback.GUID),
		SourceNomCode: firstNonEmpty(toString(mapped["sourceNomcode"]), toString(mapped["barcode"]), fallback.NomCode),
		ISBN:          firstNonEmpty(toString(mapped["isbn"]), fallback.ISBN, toString(source["isbn"]), toString(source["gtin"])),

		Name:           firstNonEmpty(toString(mapped["name"]), fallback.Name, toString(source["title"])),
		AuthorCover:    firstNonEmpty(toString(mapped["authorCover"]), fallback.AuthorCover),
		AuthorNames:    firstNonEmptySlice(toStringSlice(mapped["authorNames"]), fallback.AuthorNames),
		Annotation:     firstNonEmpty(toString(mapped["annotation"]), fallback.Annotation, toString(source["description"])),
		CoverURL:       firstNonEmpty(toString(mapped["coverUrl"]), fallback.CoverURL, toString(source["image"])),
		AgeRestriction: firstNonEmpty(toString(mapped["ageRestriction"]), fallback.AgeRestriction),
		SubjectName:    firstNonEmpty(toString(mapped["subjectName"]), fallback.SubjectName),
		NicheName:      firstNonEmpty(toString(mapped["nicheName"]), fallbackNicheName),
		BrandName:      firstNonEmpty(toString(mapped["brandName"]), fallback.BrandName),
		SeriesName:     firstNonEmpty(toString(mapped["seriesName"]), fallback.SerieName),
		PublisherName:  firstNonEmpty(toString(mapped["publisherName"]), fallback.PublisherName),
		CategoryPath:   path,
		UpdatedAt:      now,
	}

	if quantity := toFloat(mapped["quantity"]); quantity != nil {
		main.Quantity = *quantity
	}
	if price := toFloat(firstNonEmptyAny(mapped["price"], source["price"])); price != nil {
		main.Price = *price
	}
	if value := toString(mapped["categoryId"]); value != "" {
		if oid, err := primitive.ObjectIDFromHex(value); err == nil {
			main.CategoryID = oid
		}
	}

	if strings.TrimSpace(main.Name) == "" {
		return models.MainProduct{}, false
	}
	return main, true
}

func parseCategoryPath(value any) []string {
	text := strings.TrimSpace(toString(value))
	if text == "" {
		return nil
	}
	separators := []string{">", "|", "/", ","}
	parts := []string{text}
	for _, sep := range separators {
		if strings.Contains(text, sep) {
			parts = strings.Split(text, sep)
			break
		}
	}
	result := make([]string, 0, len(parts))
	for _, item := range parts {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		result = append(result, trimmed)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func newInvalidProduct(
	runID primitive.ObjectID,
	sourceURL string,
	syncSource string,
	reason string,
	payload any,
) models.InvalidProduct {
	return models.InvalidProduct{
		RunID:      runID,
		SourceURL:  strings.TrimSpace(sourceURL),
		SyncSource: strings.TrimSpace(syncSource),
		Error:      strings.TrimSpace(reason),
		Payload:    normalizeJSONLikeValue(payload),
	}
}

func normalizeJSONLikeValue(value any) any {
	switch typed := value.(type) {
	case nil, string, bool, int, int8, int16, int32, int64, float32, float64:
		return typed
	case json.Number:
		text := typed.String()
		if !strings.ContainsAny(text, ".eE") {
			if parsed, err := typed.Int64(); err == nil {
				return parsed
			}
		}
		if parsed, err := typed.Float64(); err == nil {
			return parsed
		}
		return text
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[key] = normalizeJSONLikeValue(item)
		}
		return result
	case []any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, normalizeJSONLikeValue(item))
		}
		return result
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func firstNonEmptyAny(values ...any) any {
	for _, value := range values {
		if value == nil {
			continue
		}
		if strings.TrimSpace(toString(value)) != "" {
			return value
		}
	}
	return nil
}

func firstNonEmptySlice(candidates ...[]string) []string {
	for _, item := range candidates {
		if len(item) > 0 {
			return item
		}
	}
	return nil
}

func toStringSlice(value any) []string {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case []string:
		return cleanStringSlice(typed)
	case []any:
		result := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := toString(item); text != "" {
				result = append(result, text)
			}
		}
		return cleanStringSlice(result)
	case string:
		text := strings.TrimSpace(typed)
		if text == "" {
			return nil
		}
		for _, sep := range []string{",", ";", "|"} {
			if strings.Contains(text, sep) {
				parts := strings.Split(text, sep)
				return cleanStringSlice(parts)
			}
		}
		return []string{text}
	default:
		text := toString(typed)
		if text == "" {
			return nil
		}
		return []string{text}
	}
}

func cleanStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	result := make([]string, 0, len(values))
	for _, item := range values {
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

func syntheticID(prefix, seed string) string {
	if seed == "" {
		seed = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	hash := sha1.Sum([]byte(seed))
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(hash[:8]))
}

func normalizeURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("sourceUrl is required")
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "https://" + raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", errors.New("sourceUrl is invalid")
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("sourceUrl is invalid")
	}
	if parsed.RawQuery != "" {
		query := parsed.Query()
		for key := range query {
			normalizedKey := strings.ToLower(strings.TrimSpace(key))
			if strings.HasPrefix(normalizedKey, "utm_") ||
				normalizedKey == "srsltid" ||
				normalizedKey == "gclid" ||
				normalizedKey == "fbclid" ||
				normalizedKey == "_ga" ||
				normalizedKey == "_gl" ||
				normalizedKey == "ymclid" {
				query.Del(key)
			}
		}
		parsed.RawQuery = query.Encode()
	}
	return parsed.String(), nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
