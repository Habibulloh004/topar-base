package services

import (
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"topar/backend/internal/config"
	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"go.mongodb.org/mongo-driver/bson"
)

const (
	eksmoProductsSyncKey   = "eksmo_products"
	eksmoAuthorsSyncKey    = "eksmo_authors"
	eksmoTagsSyncKey       = "eksmo_tags"
	eksmoSeriesSyncKey     = "eksmo_series"
	eksmoPublishersSyncKey = "eksmo_publishers"
)

type EksmoService struct {
	cfg        config.Config
	httpClient *http.Client
	stateRepo  *repository.SyncStateRepository

	productSyncGuardMu sync.Mutex
	productSyncRunning bool
}

type EksmoSyncOptions struct {
	PerPage  int
	MaxPages int
	Resume   bool
	Reset    bool
}

type retryableError struct {
	err error
}

func (e retryableError) Error() string {
	return e.err.Error()
}

type eksmoAPIResponse struct {
	Items      []map[string]any `json:"items"`
	Pagination struct {
		Next string `json:"next"`
		Meta struct {
			TotalItems int `json:"total_items"`
		} `json:"meta"`
	} `json:"pagination"`
}

func NewEksmoService(cfg config.Config, stateRepo *repository.SyncStateRepository) *EksmoService {
	requestTimeout := time.Duration(cfg.EksmoTimeoutS) * time.Second
	if requestTimeout <= 0 {
		requestTimeout = 300 * time.Second
	}

	dialTimeout := 20 * time.Second
	if requestTimeout < dialTimeout {
		dialTimeout = requestTimeout
	}

	tlsHandshakeTimeout := 45 * time.Second
	if requestTimeout < tlsHandshakeTimeout {
		tlsHandshakeTimeout = requestTimeout
	}

	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// Force IPv4 to avoid broken/slow IPv6 routes on some VPS networks.
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp4", addr)
		},
		ForceAttemptHTTP2:     false,
		TLSNextProto:          map[string]func(string, *tls.Conn) http.RoundTripper{},
		MaxIdleConns:          100,
		MaxConnsPerHost:       20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: requestTimeout,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &EksmoService{
		cfg: cfg,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   requestTimeout + 5*time.Second,
		},
		stateRepo: stateRepo,
	}
}

// ProductSyncRepos holds all repositories needed for product sync with entity extraction
type ProductSyncRepos struct {
	Products *repository.EksmoProductRepository
	Subjects *repository.EksmoSubjectRepository
	Niches   *repository.EksmoNicheRepository
}

func (s *EksmoService) eksmoAPIKeys() []string {
	result := make([]string, 0, len(s.cfg.EksmoAPIKeys)+1)
	seen := make(map[string]struct{}, len(s.cfg.EksmoAPIKeys)+1)

	for _, key := range s.cfg.EksmoAPIKeys {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}

	legacy := strings.TrimSpace(s.cfg.EksmoAPIKey)
	if legacy != "" {
		if _, exists := seen[legacy]; !exists {
			result = append(result, legacy)
		}
	}

	return result
}

func (s *EksmoService) primaryEksmoAPIKey() string {
	keys := s.eksmoAPIKeys()
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

func buildScopedSyncStateKey(baseKey, apiKey string) string {
	hash := sha1.Sum([]byte(strings.TrimSpace(apiKey)))
	return fmt.Sprintf("%s:%x", baseKey, hash[:6])
}

func (s *EksmoService) SyncAllProducts(ctx context.Context, repo *repository.EksmoProductRepository, options EksmoSyncOptions) (models.EksmoSyncResult, error) {
	return s.SyncAllProductsWithExtraction(ctx, ProductSyncRepos{Products: repo}, options)
}

func (s *EksmoService) SyncAllProductsWithExtraction(ctx context.Context, repos ProductSyncRepos, options EksmoSyncOptions) (models.EksmoSyncResult, error) {
	repo := repos.Products
	result := models.EksmoSyncResult{Collection: "eksmo_products"}

	if repo == nil {
		return result, fmt.Errorf("eksmo product repository is required")
	}

	apiKeys := s.eksmoAPIKeys()
	if len(apiKeys) == 0 {
		return result, fmt.Errorf("EKSMO_API_KEY (or EKSMO_API_KEYS) is required")
	}

	s.productSyncGuardMu.Lock()
	if s.productSyncRunning {
		s.productSyncGuardMu.Unlock()
		return result, fmt.Errorf("eksmo products sync is already running")
	}
	s.productSyncRunning = true
	s.productSyncGuardMu.Unlock()
	defer func() {
		s.productSyncGuardMu.Lock()
		s.productSyncRunning = false
		s.productSyncGuardMu.Unlock()
	}()

	perPage := options.PerPage
	if perPage <= 0 {
		perPage = s.cfg.EksmoPerPage
	}
	if perPage <= 0 {
		perPage = 500
	}
	if perPage > 5000 {
		perPage = 5000
	}

	maxPages := options.MaxPages
	if maxPages < 0 {
		maxPages = 0
	}
	if maxPages == 0 {
		maxPages = s.cfg.EksmoMaxPages
	}
	if maxPages < 0 {
		maxPages = 0
	}

	log.Printf("eksmo products sync: ensuring indexes")
	if err := repo.EnsureIndexes(ctx); err != nil {
		return result, err
	}
	log.Printf("eksmo products sync: product indexes ready")
	if s.stateRepo != nil {
		log.Printf("eksmo products sync: ensuring sync state indexes")
		if err := s.stateRepo.EnsureIndexes(ctx); err != nil {
			return result, err
		}
		log.Printf("eksmo products sync: sync state indexes ready")
	}

	remainingPages := maxPages
	allCompleted := true
	allAlreadyCompleted := true

	for keyIndex, apiKey := range apiKeys {
		stateKey := eksmoProductsSyncKey
		if keyIndex > 0 {
			stateKey = buildScopedSyncStateKey(eksmoProductsSyncKey, apiKey)
		}

		if options.Reset && s.stateRepo != nil {
			if err := s.stateRepo.Reset(ctx, stateKey); err != nil {
				return result, err
			}
		}

		nextURL := ""
		keyAlreadyCompleted := false
		if options.Resume && s.stateRepo != nil {
			state, err := s.stateRepo.Get(ctx, stateKey)
			if err != nil {
				return result, err
			}
			if state != nil {
				if state.Completed && state.NextURL == "" {
					keyAlreadyCompleted = true
					log.Printf("eksmo products sync: key #%d already completed, skipping", keyIndex+1)
				} else {
					nextURL = state.NextURL
				}
			}
		}

		if keyAlreadyCompleted {
			continue
		}
		allAlreadyCompleted = false

		if nextURL == "" {
			initialURL, err := s.buildInitialURLWithKey("products", perPage, apiKey)
			if err != nil {
				return result, err
			}
			nextURL = initialURL
		}

		currentPerPage := perPage
		gotTotalForThisKey := false

		for nextURL != "" {
			if maxPages > 0 && remainingPages <= 0 {
				result.StoppedEarly = true
				result.NextURL = nextURL
				allCompleted = false
				if s.stateRepo != nil {
					_ = s.stateRepo.Upsert(ctx, stateKey, nextURL, false)
				}
				break
			}

			log.Printf(
				"eksmo products sync request #%d started: key=%d/%d per_page=%d",
				result.Pages+1,
				keyIndex+1,
				len(apiKeys),
				currentPerPage,
			)

			payload, usedPerPage, err := s.fetchPageResilientWithKey(ctx, nextURL, currentPerPage, apiKey)
			if err != nil {
				log.Printf("eksmo products sync request #%d failed: %v", result.Pages+1, err)
				if s.stateRepo != nil {
					_ = s.stateRepo.Upsert(ctx, stateKey, nextURL, false)
				}
				result.NextURL = nextURL
				result.Completed = false
				return result, err
			}
			currentPerPage = usedPerPage

			products := make([]models.EksmoProduct, 0, len(payload.Items))
			for _, item := range payload.Items {
				products = append(products, mapEksmoItem(item))
			}

			upserted, modified, skipped, err := repo.UpsertBatch(ctx, products)
			if err != nil {
				return result, err
			}

			// Extract and upsert subjects and niches from products
			if repos.Subjects != nil {
				subjects := extractSubjectsFromProducts(products)
				if len(subjects) > 0 {
					_, _, _ = repos.Subjects.UpsertBatch(ctx, subjects)
				}
			}
			if repos.Niches != nil {
				niches := extractNichesFromProducts(products)
				if len(niches) > 0 {
					_, _, _ = repos.Niches.UpsertBatch(ctx, niches)
				}
			}

			result.Pages++
			result.Fetched += len(products)
			result.Upserted += upserted
			result.Modified += modified
			result.Skipped += skipped
			if maxPages > 0 {
				remainingPages--
			}

			log.Printf(
				"eksmo products sync request #%d: key=%d/%d fetched=%d per_page=%d total_fetched=%d next=%t",
				result.Pages,
				keyIndex+1,
				len(apiKeys),
				len(products),
				currentPerPage,
				result.Fetched,
				payload.Pagination.Next != "",
			)
			if !gotTotalForThisKey && payload.Pagination.Meta.TotalItems > 0 {
				result.TotalInAPI += payload.Pagination.Meta.TotalItems
				gotTotalForThisKey = true
			}

			nextURL = payload.Pagination.Next
			result.NextURL = nextURL

			if s.stateRepo != nil {
				if err := s.stateRepo.Upsert(ctx, stateKey, nextURL, nextURL == ""); err != nil {
					return result, err
				}
			}
		}

		if result.StoppedEarly {
			break
		}

		if nextURL != "" {
			allCompleted = false
		}
	}

	if allAlreadyCompleted && options.Resume {
		result.Completed = true
		result.Message = "Eksmo sync already completed. Use reset=1 to start from beginning."
		return result, nil
	}

	result.Completed = allCompleted && !result.StoppedEarly
	if result.Completed {
		result.NextURL = ""
		result.Message = "Eksmo products synced successfully"
	} else if result.StoppedEarly {
		result.Message = "Chunk synced. Call /syncEksmoProducts again to continue from saved cursor"
	} else {
		result.Message = "Eksmo sync paused"
	}

	return result, nil
}

// SyncAllAuthors syncs authors from the Eksmo API
func (s *EksmoService) SyncAllAuthors(ctx context.Context, repo *repository.EksmoAuthorRepository, options EksmoSyncOptions) (models.EksmoAuthorSyncResult, error) {
	result := models.EksmoAuthorSyncResult{Collection: "eksmo_authors"}

	if strings.TrimSpace(s.primaryEksmoAPIKey()) == "" {
		return result, fmt.Errorf("EKSMO_API_KEY (or EKSMO_API_KEYS) is required")
	}

	perPage := options.PerPage
	if perPage <= 0 {
		perPage = 500
	}
	if perPage > 5000 {
		perPage = 5000
	}

	maxPages := options.MaxPages
	if maxPages < 0 {
		maxPages = 0
	}

	if err := repo.EnsureIndexes(ctx); err != nil {
		return result, err
	}

	if options.Reset && s.stateRepo != nil {
		if err := s.stateRepo.Reset(ctx, eksmoAuthorsSyncKey); err != nil {
			return result, err
		}
	}

	nextURL := ""
	if options.Resume && s.stateRepo != nil {
		state, err := s.stateRepo.Get(ctx, eksmoAuthorsSyncKey)
		if err != nil {
			return result, err
		}
		if state != nil {
			if state.Completed && state.NextURL == "" {
				result.Completed = true
				result.Message = "Authors sync already completed. Use reset=1 to start from beginning."
				return result, nil
			}
			nextURL = state.NextURL
		}
	}

	if nextURL == "" {
		initialURL, err := s.buildInitialURL("authors", perPage)
		if err != nil {
			return result, err
		}
		nextURL = initialURL
	}

	currentPerPage := perPage
	for nextURL != "" {
		if maxPages > 0 && result.Pages >= maxPages {
			break
		}

		payload, usedPerPage, err := s.fetchPageResilient(ctx, nextURL, currentPerPage)
		if err != nil {
			if s.stateRepo != nil {
				_ = s.stateRepo.Upsert(ctx, eksmoAuthorsSyncKey, nextURL, false)
			}
			result.NextURL = nextURL
			result.Completed = false
			return result, err
		}
		currentPerPage = usedPerPage

		authors := make([]models.EksmoAuthor, 0, len(payload.Items))
		for _, item := range payload.Items {
			authors = append(authors, mapEksmoAuthor(item))
		}

		upserted, modified, err := repo.UpsertBatch(ctx, authors)
		if err != nil {
			return result, err
		}

		result.Pages++
		result.Fetched += len(authors)
		result.Upserted += upserted
		result.Modified += modified
		if payload.Pagination.Meta.TotalItems > 0 {
			result.TotalInAPI = payload.Pagination.Meta.TotalItems
		}

		nextURL = payload.Pagination.Next
		result.NextURL = nextURL
		result.Completed = nextURL == ""

		if s.stateRepo != nil {
			if err := s.stateRepo.Upsert(ctx, eksmoAuthorsSyncKey, nextURL, result.Completed); err != nil {
				return result, err
			}
		}
	}

	if result.Completed {
		result.Message = "Eksmo authors synced successfully"
	} else {
		result.Message = "Authors sync paused"
	}

	return result, nil
}

// SyncAllTags syncs tags from the Eksmo API
func (s *EksmoService) SyncAllTags(ctx context.Context, repo *repository.EksmoTagRepository, options EksmoSyncOptions) (models.EksmoTagSyncResult, error) {
	result := models.EksmoTagSyncResult{Collection: "eksmo_tags"}

	if strings.TrimSpace(s.primaryEksmoAPIKey()) == "" {
		return result, fmt.Errorf("EKSMO_API_KEY (or EKSMO_API_KEYS) is required")
	}

	perPage := options.PerPage
	if perPage <= 0 {
		perPage = 500
	}

	maxPages := options.MaxPages
	if maxPages < 0 {
		maxPages = 0
	}

	if err := repo.EnsureIndexes(ctx); err != nil {
		return result, err
	}

	if options.Reset && s.stateRepo != nil {
		if err := s.stateRepo.Reset(ctx, eksmoTagsSyncKey); err != nil {
			return result, err
		}
	}

	nextURL := ""
	if options.Resume && s.stateRepo != nil {
		state, err := s.stateRepo.Get(ctx, eksmoTagsSyncKey)
		if err != nil {
			return result, err
		}
		if state != nil {
			if state.Completed && state.NextURL == "" {
				result.Completed = true
				result.Message = "Tags sync already completed. Use reset=1 to start from beginning."
				return result, nil
			}
			nextURL = state.NextURL
		}
	}

	if nextURL == "" {
		initialURL, err := s.buildInitialURL("tags", perPage)
		if err != nil {
			return result, err
		}
		nextURL = initialURL
	}

	currentPerPage := perPage
	for nextURL != "" {
		if maxPages > 0 && result.Pages >= maxPages {
			break
		}

		payload, usedPerPage, err := s.fetchPageResilient(ctx, nextURL, currentPerPage)
		if err != nil {
			if s.stateRepo != nil {
				_ = s.stateRepo.Upsert(ctx, eksmoTagsSyncKey, nextURL, false)
			}
			result.NextURL = nextURL
			result.Completed = false
			return result, err
		}
		currentPerPage = usedPerPage

		tags := make([]models.EksmoTag, 0, len(payload.Items))
		for _, item := range payload.Items {
			tags = append(tags, mapEksmoTag(item))
		}

		upserted, modified, err := repo.UpsertBatch(ctx, tags)
		if err != nil {
			return result, err
		}

		result.Pages++
		result.Fetched += len(tags)
		result.Upserted += upserted
		result.Modified += modified
		if payload.Pagination.Meta.TotalItems > 0 {
			result.TotalInAPI = payload.Pagination.Meta.TotalItems
		}

		nextURL = payload.Pagination.Next
		result.NextURL = nextURL
		result.Completed = nextURL == ""

		if s.stateRepo != nil {
			if err := s.stateRepo.Upsert(ctx, eksmoTagsSyncKey, nextURL, result.Completed); err != nil {
				return result, err
			}
		}
	}

	if result.Completed {
		result.Message = "Eksmo tags synced successfully"
	} else {
		result.Message = "Tags sync paused"
	}

	return result, nil
}

// SyncAllSeries syncs series from the Eksmo API
func (s *EksmoService) SyncAllSeries(ctx context.Context, repo *repository.EksmoSeriesRepository, options EksmoSyncOptions) (models.EksmoSeriesSyncResult, error) {
	result := models.EksmoSeriesSyncResult{Collection: "eksmo_series"}

	if strings.TrimSpace(s.primaryEksmoAPIKey()) == "" {
		return result, fmt.Errorf("EKSMO_API_KEY (or EKSMO_API_KEYS) is required")
	}

	perPage := options.PerPage
	if perPage <= 0 {
		perPage = 500
	}

	maxPages := options.MaxPages
	if maxPages < 0 {
		maxPages = 0
	}

	if err := repo.EnsureIndexes(ctx); err != nil {
		return result, err
	}

	if options.Reset && s.stateRepo != nil {
		if err := s.stateRepo.Reset(ctx, eksmoSeriesSyncKey); err != nil {
			return result, err
		}
	}

	nextURL := ""
	if options.Resume && s.stateRepo != nil {
		state, err := s.stateRepo.Get(ctx, eksmoSeriesSyncKey)
		if err != nil {
			return result, err
		}
		if state != nil {
			if state.Completed && state.NextURL == "" {
				result.Completed = true
				result.Message = "Series sync already completed. Use reset=1 to start from beginning."
				return result, nil
			}
			nextURL = state.NextURL
		}
	}

	if nextURL == "" {
		initialURL, err := s.buildInitialURL("series", perPage)
		if err != nil {
			return result, err
		}
		nextURL = initialURL
	}

	currentPerPage := perPage
	for nextURL != "" {
		if maxPages > 0 && result.Pages >= maxPages {
			break
		}

		payload, usedPerPage, err := s.fetchPageResilient(ctx, nextURL, currentPerPage)
		if err != nil {
			if s.stateRepo != nil {
				_ = s.stateRepo.Upsert(ctx, eksmoSeriesSyncKey, nextURL, false)
			}
			result.NextURL = nextURL
			result.Completed = false
			return result, err
		}
		currentPerPage = usedPerPage

		seriesList := make([]models.EksmoSeries, 0, len(payload.Items))
		for _, item := range payload.Items {
			seriesList = append(seriesList, mapEksmoSeries(item))
		}

		upserted, modified, err := repo.UpsertBatch(ctx, seriesList)
		if err != nil {
			return result, err
		}

		result.Pages++
		result.Fetched += len(seriesList)
		result.Upserted += upserted
		result.Modified += modified
		if payload.Pagination.Meta.TotalItems > 0 {
			result.TotalInAPI = payload.Pagination.Meta.TotalItems
		}

		nextURL = payload.Pagination.Next
		result.NextURL = nextURL
		result.Completed = nextURL == ""

		if s.stateRepo != nil {
			if err := s.stateRepo.Upsert(ctx, eksmoSeriesSyncKey, nextURL, result.Completed); err != nil {
				return result, err
			}
		}
	}

	if result.Completed {
		result.Message = "Eksmo series synced successfully"
	} else {
		result.Message = "Series sync paused"
	}

	return result, nil
}

// SyncAllPublishers syncs publishers from the Eksmo API
func (s *EksmoService) SyncAllPublishers(ctx context.Context, repo *repository.EksmoPublisherRepository, options EksmoSyncOptions) (models.EksmoPublisherSyncResult, error) {
	result := models.EksmoPublisherSyncResult{Collection: "eksmo_publishers"}

	if strings.TrimSpace(s.primaryEksmoAPIKey()) == "" {
		return result, fmt.Errorf("EKSMO_API_KEY (or EKSMO_API_KEYS) is required")
	}

	perPage := options.PerPage
	if perPage <= 0 {
		perPage = 500
	}

	maxPages := options.MaxPages
	if maxPages < 0 {
		maxPages = 0
	}

	if err := repo.EnsureIndexes(ctx); err != nil {
		return result, err
	}

	if options.Reset && s.stateRepo != nil {
		if err := s.stateRepo.Reset(ctx, eksmoPublishersSyncKey); err != nil {
			return result, err
		}
	}

	nextURL := ""
	if options.Resume && s.stateRepo != nil {
		state, err := s.stateRepo.Get(ctx, eksmoPublishersSyncKey)
		if err != nil {
			return result, err
		}
		if state != nil {
			if state.Completed && state.NextURL == "" {
				result.Completed = true
				result.Message = "Publishers sync already completed. Use reset=1 to start from beginning."
				return result, nil
			}
			nextURL = state.NextURL
		}
	}

	if nextURL == "" {
		initialURL, err := s.buildInitialURL("publishers", perPage)
		if err != nil {
			return result, err
		}
		nextURL = initialURL
	}

	currentPerPage := perPage
	for nextURL != "" {
		if maxPages > 0 && result.Pages >= maxPages {
			break
		}

		payload, usedPerPage, err := s.fetchPageResilient(ctx, nextURL, currentPerPage)
		if err != nil {
			if s.stateRepo != nil {
				_ = s.stateRepo.Upsert(ctx, eksmoPublishersSyncKey, nextURL, false)
			}
			result.NextURL = nextURL
			result.Completed = false
			return result, err
		}
		currentPerPage = usedPerPage

		publishers := make([]models.EksmoPublisher, 0, len(payload.Items))
		for _, item := range payload.Items {
			publishers = append(publishers, mapEksmoPublisher(item))
		}

		upserted, modified, err := repo.UpsertBatch(ctx, publishers)
		if err != nil {
			return result, err
		}

		result.Pages++
		result.Fetched += len(publishers)
		result.Upserted += upserted
		result.Modified += modified
		if payload.Pagination.Meta.TotalItems > 0 {
			result.TotalInAPI = payload.Pagination.Meta.TotalItems
		}

		nextURL = payload.Pagination.Next
		result.NextURL = nextURL
		result.Completed = nextURL == ""

		if s.stateRepo != nil {
			if err := s.stateRepo.Upsert(ctx, eksmoPublishersSyncKey, nextURL, result.Completed); err != nil {
				return result, err
			}
		}
	}

	if result.Completed {
		result.Message = "Eksmo publishers synced successfully"
	} else {
		result.Message = "Publishers sync paused"
	}

	return result, nil
}

func (s *EksmoService) fetchPageResilient(ctx context.Context, endpoint string, preferredPerPage int) (eksmoAPIResponse, int, error) {
	return s.fetchPageResilientWithKey(ctx, endpoint, preferredPerPage, s.primaryEksmoAPIKey())
}

func (s *EksmoService) fetchPageResilientWithKey(ctx context.Context, endpoint string, preferredPerPage int, apiKey string) (eksmoAPIResponse, int, error) {
	if strings.TrimSpace(apiKey) == "" {
		return eksmoAPIResponse{}, 0, fmt.Errorf("EKSMO_API_KEY (or EKSMO_API_KEYS) is required")
	}

	candidates := buildPerPageCandidates(preferredPerPage)
	var lastErr error

	for _, perPage := range candidates {
		tunedEndpoint, err := applyPerPageToEndpoint(endpoint, perPage, apiKey)
		if err != nil {
			return eksmoAPIResponse{}, 0, err
		}

		payload, err := s.fetchPageWithRetry(ctx, tunedEndpoint)
		if err == nil {
			return payload, perPage, nil
		}

		lastErr = err
		if !isRetryable(err) {
			return eksmoAPIResponse{}, 0, err
		}
	}

	return eksmoAPIResponse{}, 0, fmt.Errorf("failed to fetch eksmo page with adaptive per_page: %w", lastErr)
}

func (s *EksmoService) fetchPageWithRetry(ctx context.Context, endpoint string) (eksmoAPIResponse, error) {
	requestTimeout := time.Duration(s.cfg.EksmoTimeoutS) * time.Second
	if requestTimeout <= 0 {
		requestTimeout = 300 * time.Second
	}

	retries := s.cfg.EksmoRetries
	if retries < 0 {
		retries = 0
	}

	backoff := time.Duration(s.cfg.EksmoBackoffMS) * time.Millisecond
	if backoff <= 0 {
		backoff = 1500 * time.Millisecond
	}

	var lastErr error
	for attempt := 0; attempt <= retries; attempt++ {
		log.Printf(
			"eksmo request attempt %d/%d: %s (timeout=%s)",
			attempt+1,
			retries+1,
			redactEksmoURL(endpoint),
			requestTimeout,
		)
		attemptCtx, cancel := context.WithTimeout(ctx, requestTimeout)
		payload, err := s.fetchPage(attemptCtx, endpoint)
		cancel()
		if err == nil {
			return payload, nil
		}
		log.Printf("eksmo request attempt %d failed: %v", attempt+1, err)

		lastErr = err
		if attempt == retries || !isRetryable(err) {
			break
		}

		if err := sleepWithContext(ctx, backoff*time.Duration(attempt+1)); err != nil {
			return eksmoAPIResponse{}, err
		}
	}

	return eksmoAPIResponse{}, fmt.Errorf("eksmo page request failed after %d attempt(s): %w", retries+1, lastErr)
}

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isRetryable(err error) bool {
	var marked retryableError
	if errors.As(err, &marked) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func applyPerPageToEndpoint(endpoint string, perPage int, eksmoAPIKey string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Set("per_page", strconv.Itoa(perPage))
	if strings.TrimSpace(eksmoAPIKey) != "" {
		query.Set("key", eksmoAPIKey)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func buildPerPageCandidates(preferred int) []int {
	if preferred <= 0 {
		preferred = 500
	}
	if preferred > 5000 {
		preferred = 5000
	}

	tiered := []int{preferred, 400, 300, 250, 200, 150, 100, 75, 50, 30, 20, 10, 5, 3, 2, 1}
	result := make([]int, 0, len(tiered))
	seen := map[int]struct{}{}
	for _, value := range tiered {
		if value > preferred {
			continue
		}
		if value < 1 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	if len(result) == 0 {
		return []int{preferred}
	}
	return result
}

func (s *EksmoService) buildInitialURL(endpoint string, perPage int) (string, error) {
	return s.buildInitialURLWithKey(endpoint, perPage, s.primaryEksmoAPIKey())
}

func (s *EksmoService) buildInitialURLWithKey(endpoint string, perPage int, apiKey string) (string, error) {
	baseURL := strings.TrimRight(s.cfg.EksmoBaseURL, "/") + "/v3/" + endpoint + "/"
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	if strings.TrimSpace(apiKey) != "" {
		query.Set("key", apiKey)
	}
	query.Set("per_page", strconv.Itoa(perPage))
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func (s *EksmoService) fetchPage(ctx context.Context, endpoint string) (eksmoAPIResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return eksmoAPIResponse{}, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "topar-sync/1.0")

	response, err := s.httpClient.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return eksmoAPIResponse{}, retryableError{err: err}
		}
		var netErr net.Error
		if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
			return eksmoAPIResponse{}, retryableError{err: err}
		}
		return eksmoAPIResponse{}, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		statusErr := fmt.Errorf("eksmo api status %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= http.StatusInternalServerError {
			return eksmoAPIResponse{}, retryableError{err: statusErr}
		}
		return eksmoAPIResponse{}, statusErr
	}

	var payload eksmoAPIResponse
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return eksmoAPIResponse{}, err
	}

	return payload, nil
}

func redactEksmoURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "invalid-url"
	}
	query := parsed.Query()
	if query.Has("key") {
		query.Set("key", "***")
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// mapEksmoItem maps API response to EksmoProduct with all fields
func mapEksmoItem(item map[string]any) models.EksmoProduct {
	now := time.Now().UTC()

	product := models.EksmoProduct{
		GUID:        readString(item, "GUID"),
		GUIDNOM:     readString(item, "GUID_NOM"),
		NomCode:     readString(item, "NOMCODE"),
		Name:        readString(item, "NAME"),
		ISBN:        readString(item, "ISBN"),
		AuthorCover: readString(item, "AUTHOR_COVER"),
		Annotation:  readString(item, "ANNOTACIA"),
		Raw:         bson.M(item),
		SyncedAt:    now,
		UpdatedAt:   now,
	}

	// Extract Subject
	if subjectData, ok := item["SUBJECT"].(map[string]any); ok {
		product.Subject = &models.EksmoSubject{
			GUID:      readString(subjectData, "GUID"),
			Name:      readString(subjectData, "NAME"),
			OwnerGUID: readString(subjectData, "OWNER_GUID"),
		}
		product.SubjectName = product.Subject.Name
	}

	// Extract Niche
	if nicheData, ok := item["NICHE"].(map[string]any); ok {
		product.Niche = &models.EksmoNiche{
			GUID:      readString(nicheData, "GUID"),
			Name:      readString(nicheData, "NAME"),
			OwnerGUID: readString(nicheData, "OWNER_GUID"),
		}
	}

	// Extract Brand
	if brandData, ok := item["IZDAT_BRAND"].(map[string]any); ok {
		product.Brand = &models.EksmoBrand{
			GUID: readString(brandData, "GUID"),
			Name: readString(brandData, "NAME"),
		}
		product.BrandName = product.Brand.Name
	}

	// Extract Series
	if serieData, ok := item["SERIE"].(map[string]any); ok {
		product.Series = &models.EksmoProductSeriesRef{
			GUID: readString(serieData, "GUID"),
			Name: readString(serieData, "NAME"),
		}
		product.SerieName = product.Series.Name
	}

	// Extract Publisher
	if pubData, ok := item["PUBLISHER"].(map[string]any); ok {
		product.Publisher = &models.EksmoProductPublisherRef{
			GUID: readString(pubData, "GUID"),
			Name: readString(pubData, "NAME"),
		}
		product.PublisherName = product.Publisher.Name
	}

	// Extract Authors array
	if authorArray, ok := item["AUTHOR"].([]any); ok {
		var authorRefs []models.EksmoProductAuthorRef
		var authorNames []string
		for _, authorItem := range authorArray {
			if authorData, ok := authorItem.(map[string]any); ok {
				ref := models.EksmoProductAuthorRef{
					GUID:         readString(authorData, "GUID"),
					Code:         readString(authorData, "CODE"),
					Name:         readString(authorData, "NAME"),
					IsWriter:     readBool(authorData, "IS_PISATEL"),
					IsTranslator: readBool(authorData, "IS_PEREVODCHIK"),
					IsArtist:     readBool(authorData, "IS_HUDOJNIK"),
				}
				if ref.GUID != "" {
					authorRefs = append(authorRefs, ref)
				}
				if ref.Name != "" {
					authorNames = append(authorNames, ref.Name)
				}
			}
		}
		product.AuthorRefs = authorRefs
		product.AuthorNames = authorNames
	}

	// Extract Tags array
	if tagArray, ok := item["TAGS"].([]any); ok {
		var tagRefs []models.EksmoProductTagRef
		var tagNames []string
		for _, tagItem := range tagArray {
			if tagData, ok := tagItem.(map[string]any); ok {
				ref := models.EksmoProductTagRef{
					GUID: readString(tagData, "GUID"),
					Name: readString(tagData, "NAME"),
				}
				if ref.GUID != "" {
					tagRefs = append(tagRefs, ref)
				}
				if ref.Name != "" {
					tagNames = append(tagNames, ref.Name)
				}
			}
		}
		product.TagRefs = tagRefs
		product.TagNames = tagNames
	}

	// Extract Genres array
	if genreArray, ok := item["GENRE"].([]any); ok {
		var genreRefs []models.EksmoProductGenreRef
		var genreNames []string
		for _, genreItem := range genreArray {
			if genreData, ok := genreItem.(map[string]any); ok {
				ref := models.EksmoProductGenreRef{
					GUID: readString(genreData, "GUID"),
					Name: readString(genreData, "NAME"),
				}
				genreRefs = append(genreRefs, ref)
				if ref.Name != "" {
					genreNames = append(genreNames, ref.Name)
				}
			}
		}
		product.GenreRefs = genreRefs
		product.GenreNames = genreNames
	}

	// Physical attributes
	product.Pages = readInt(item, "PAGES")
	product.Format = readString(item, "FORMAT")
	product.PaperType = readString(item, "PAPER")
	product.BindingType = readString(item, "PEREPLET_TYPE")
	product.AgeRestriction = readString(item, "VOZRASTNOE_OGRANICHENIE")

	// Covers
	product.CoverURL = readCoverURL(item)
	product.Covers = extractAllCovers(item)

	if product.Name == "" {
		product.Name = readString(item, "NAME_FOR_PRICE")
	}

	return product
}

// mapEksmoAuthor maps API response to EksmoAuthor
func mapEksmoAuthor(item map[string]any) models.EksmoAuthor {
	now := time.Now().UTC()

	author := models.EksmoAuthor{
		GUID:       readString(item, "GUID"),
		Code:       readString(item, "CODE"),
		Name:       readString(item, "NAME"),
		FirstName:  readString(item, "FIRST_NAME"),
		Surname:    readString(item, "SURNAME"),
		SecondName: readString(item, "SECOND_NAME"),
		DateBirth:  readString(item, "DATE_BIRTH"),
		DateDeath:  readString(item, "DATE_DEATH"),
		SyncedAt:   now,
		UpdatedAt:  now,
	}

	// Parse FLAGS object
	if flags, ok := item["FLAGS"].(map[string]any); ok {
		author.IsWriter = readBool(flags, "WRITER")
		author.IsTranslator = readBool(flags, "TRANSLATOR")
		author.IsArtist = readBool(flags, "PAINTER")
		author.IsSpeaker = readBool(flags, "SPEAKER")
		author.IsRedactor = readBool(flags, "REDACTOR")
		author.IsCompiler = readBool(flags, "COMPILLER")
	} else {
		// Fallback to direct fields
		author.IsWriter = readBool(item, "IS_PISATEL")
		author.IsTranslator = readBool(item, "IS_PEREVODCHIK")
		author.IsArtist = readBool(item, "IS_HUDOJNIK")
	}

	return author
}

// mapEksmoTag maps API response to EksmoTag
func mapEksmoTag(item map[string]any) models.EksmoTag {
	now := time.Now().UTC()

	tag := models.EksmoTag{
		GUID:      readString(item, "GUID"),
		Code:      readString(item, "CODE"),
		Name:      readString(item, "NAME"),
		IsActive:  !readBool(item, "IS_NO_ACTIVE"),
		SyncedAt:  now,
		UpdatedAt: now,
	}

	// Extract category GUID
	if categoryData, ok := item["CATEGORY"].(map[string]any); ok {
		tag.CategoryGUID = readString(categoryData, "GUID")
	}

	return tag
}

// mapEksmoSeries maps API response to EksmoSeries
func mapEksmoSeries(item map[string]any) models.EksmoSeries {
	now := time.Now().UTC()

	series := models.EksmoSeries{
		GUID:        readString(item, "GUID"),
		Code:        readString(item, "CODE"),
		Name:        readString(item, "NAME"),
		Description: readString(item, "DESCRIPTION"),
		SyncedAt:    now,
		UpdatedAt:   now,
	}

	// Extract organization data
	if orgData, ok := item["ORGANIZATION"].(map[string]any); ok {
		series.OrganizationGUID = readString(orgData, "GUID")
		series.OrganizationName = readString(orgData, "NAME")
	}

	return series
}

// mapEksmoPublisher maps API response to EksmoPublisher
func mapEksmoPublisher(item map[string]any) models.EksmoPublisher {
	now := time.Now().UTC()

	return models.EksmoPublisher{
		GUID:      readString(item, "GUID"),
		Code:      readString(item, "CODE"),
		Name:      readString(item, "NAME"),
		SyncedAt:  now,
		UpdatedAt: now,
	}
}

func readString(data map[string]any, key string) string {
	raw, ok := data[key]
	if !ok || raw == nil {
		return ""
	}
	return castToString(raw)
}

func readNestedString(data map[string]any, key string, nestedKey string) string {
	raw, ok := data[key]
	if !ok || raw == nil {
		return ""
	}
	object, ok := raw.(map[string]any)
	if !ok {
		return ""
	}
	return castToString(object[nestedKey])
}

func readBool(data map[string]any, key string) bool {
	raw, ok := data[key]
	if !ok || raw == nil {
		return false
	}
	switch v := raw.(type) {
	case bool:
		return v
	case string:
		return v == "1" || v == "true" || strings.ToLower(v) == "true"
	case float64:
		return v != 0
	case int:
		return v != 0
	}
	return false
}

func readInt(data map[string]any, key string) int {
	raw, ok := data[key]
	if !ok || raw == nil {
		return 0
	}
	switch v := raw.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case string:
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return 0
}

func readCoverURL(data map[string]any) string {
	rawCovers, ok := data["COVERS"]
	if !ok || rawCovers == nil {
		return ""
	}
	covers, ok := rawCovers.(map[string]any)
	if !ok {
		return ""
	}
	cover1, ok := covers["cover1"].(map[string]any)
	if !ok {
		return ""
	}
	return castToString(cover1["LINK"])
}

func extractAllCovers(data map[string]any) map[string]string {
	rawCovers, ok := data["COVERS"]
	if !ok || rawCovers == nil {
		return nil
	}
	covers, ok := rawCovers.(map[string]any)
	if !ok {
		return nil
	}
	result := make(map[string]string)
	for key, coverData := range covers {
		if cover, ok := coverData.(map[string]any); ok {
			if link := castToString(cover["LINK"]); link != "" {
				result[key] = link
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func castToString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case float64:
		if math.Trunc(typed) == typed {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case bool:
		if typed {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

// extractSubjectsFromProducts extracts unique subjects from a batch of products
func extractSubjectsFromProducts(products []models.EksmoProduct) []models.EksmoSubjectEntity {
	seen := make(map[string]struct{})
	var subjects []models.EksmoSubjectEntity
	now := time.Now().UTC()

	for _, p := range products {
		if p.Subject == nil || p.Subject.GUID == "" {
			continue
		}
		if _, exists := seen[p.Subject.GUID]; exists {
			continue
		}
		seen[p.Subject.GUID] = struct{}{}

		subjects = append(subjects, models.EksmoSubjectEntity{
			GUID:      p.Subject.GUID,
			Name:      p.Subject.Name,
			OwnerGUID: p.Subject.OwnerGUID,
			SyncedAt:  now,
			UpdatedAt: now,
		})
	}

	return subjects
}

// extractNichesFromProducts extracts unique niches from a batch of products
func extractNichesFromProducts(products []models.EksmoProduct) []models.EksmoNicheEntity {
	seen := make(map[string]struct{})
	var niches []models.EksmoNicheEntity
	now := time.Now().UTC()

	for _, p := range products {
		if p.Niche == nil || p.Niche.GUID == "" {
			continue
		}
		if _, exists := seen[p.Niche.GUID]; exists {
			continue
		}
		seen[p.Niche.GUID] = struct{}{}

		niches = append(niches, models.EksmoNicheEntity{
			GUID:      p.Niche.GUID,
			Name:      p.Niche.Name,
			OwnerGUID: p.Niche.OwnerGUID,
			SyncedAt:  now,
			UpdatedAt: now,
		})
	}

	return niches
}
