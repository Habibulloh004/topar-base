package handlers

import (
	"context"
	"encoding/base64"
	"errors"
	"log"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"
	"topar/backend/internal/services"

	"github.com/gofiber/fiber/v2"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type EksmoProductHandler struct {
	repo            *repository.EksmoProductRepository
	service         *services.EksmoService
	categoryLinker  *services.CategoryLinker
	authorRepo      *repository.EksmoAuthorRepository
	tagRepo         *repository.EksmoTagRepository
	seriesRepo      *repository.EksmoSeriesRepository
	publisherRepo   *repository.EksmoPublisherRepository
	subjectRepo     *repository.EksmoSubjectRepository
	nicheRepo       *repository.EksmoNicheRepository
	treeBuilder     *services.EksmoTreeBuilder
	mainProductRepo *repository.MainProductRepository
	billzSync       *services.BillzSyncService
	uploadsDir      string
	redisClient     *redis.Client
	redisCacheTTL   time.Duration
}

func NewEksmoProductHandler(
	repo *repository.EksmoProductRepository,
	service *services.EksmoService,
	categoryLinker *services.CategoryLinker,
	authorRepo *repository.EksmoAuthorRepository,
	tagRepo *repository.EksmoTagRepository,
	seriesRepo *repository.EksmoSeriesRepository,
	publisherRepo *repository.EksmoPublisherRepository,
	subjectRepo *repository.EksmoSubjectRepository,
	nicheRepo *repository.EksmoNicheRepository,
	treeBuilder *services.EksmoTreeBuilder,
	mainProductRepo *repository.MainProductRepository,
	billzSync *services.BillzSyncService,
	uploadsDir string,
	redisClient *redis.Client,
	redisCacheTTL time.Duration,
) *EksmoProductHandler {
	return &EksmoProductHandler{
		repo:            repo,
		service:         service,
		categoryLinker:  categoryLinker,
		authorRepo:      authorRepo,
		tagRepo:         tagRepo,
		seriesRepo:      seriesRepo,
		publisherRepo:   publisherRepo,
		subjectRepo:     subjectRepo,
		nicheRepo:       nicheRepo,
		treeBuilder:     treeBuilder,
		mainProductRepo: mainProductRepo,
		billzSync:       billzSync,
		uploadsDir:      strings.TrimSpace(uploadsDir),
		redisClient:     redisClient,
		redisCacheTTL:   redisCacheTTL,
	}
}

func (h *EksmoProductHandler) RegisterRoutes(app *fiber.App) {
	// Existing product routes
	app.Get("/syncEksmoProducts", h.SyncEksmoProducts)
	app.Post("/syncEksmoProducts", h.SyncEksmoProducts)
	app.Get("/eksmoProducts", h.GetEksmoProducts)
	app.Get("/eksmoProducts/duplicates", h.GetEksmoProductsDuplicates)
	app.Get("/eksmoProductsMeta", h.GetEksmoProductsMeta)
	app.Delete("/eksmoProducts", h.DeleteEksmoProducts)
	app.Delete("/eksmoProducts/:id", h.DeleteEksmoProduct)

	// New sync routes for entities
	app.Post("/syncEksmoAuthors", h.SyncEksmoAuthors)
	app.Post("/syncEksmoTags", h.SyncEksmoTags)
	app.Post("/syncEksmoSeries", h.SyncEksmoSeries)
	app.Post("/syncEksmoPublishers", h.SyncEksmoPublishers)
	app.Post("/syncAll", h.SyncAll)

	// Category linking
	app.Post("/linkProductCategories", h.LinkProductCategories)
	app.Get("/linkProductCategories", h.LinkProductCategories)

	// Entity list routes
	app.Get("/eksmoAuthors", h.GetEksmoAuthors)
	app.Get("/eksmoTags", h.GetEksmoTags)
	app.Get("/eksmoSeries", h.GetEksmoSeries)
	app.Get("/eksmoPublishers", h.GetEksmoPublishers)
	app.Get("/eksmoSubjects", h.GetEksmoSubjects)
	app.Get("/eksmoNiches", h.GetEksmoNiches)

	// Tree endpoint for hierarchical navigation
	app.Get("/eksmoNichesTree", h.GetEksmoNichesTree)

	// Copy selected or grouped Eksmo products to main_products
	app.Post("/copyEksmoProductsToMain", h.CopyEksmoProductsToMain)

	// Main products management
	app.Post("/mainProducts", h.CreateMainProduct)
	app.Put("/mainProducts/:id", h.UpdateMainProduct)
	app.Get("/mainProducts", h.GetMainProducts)
	app.Get("/mainProducts/source-categories", h.GetMainProductsSourceCategories)
	app.Post("/mainProducts/link-category", h.LinkMainProductsCategory)
	app.Post("/mainProducts/unlink-category", h.UnlinkMainProductsCategory)
	app.Get("/mainProducts/export", h.ExportMainProducts)
	app.Post("/mainProducts/import", h.ImportMainProducts)
	app.Post("/mainProducts/upload-images", h.UploadMainProductImages)
	app.Post("/syncMainProductsFromBillz", h.SyncMainProductsFromBillz)
	app.Delete("/mainProducts", h.DeleteMainProducts)
	app.Delete("/mainProducts/:id", h.DeleteMainProduct)
	app.Delete("/mainProducts/:id/category", h.RemoveMainProductCategory)
}

func (h *EksmoProductHandler) SyncEksmoProducts(c *fiber.Ctx) error {
	perPage := parseIntQuery(c, "per_page", 500)
	maxPages := parseIntQuery(c, "max_pages", 0)
	resume := parseBoolQuery(c, "resume", true)
	reset := parseBoolQuery(c, "reset", false)
	log.Printf(
		"syncEksmoProducts requested: per_page=%d max_pages=%d resume=%t reset=%t",
		perPage,
		maxPages,
		resume,
		reset,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	// Use the new extraction method to also save subjects and niches
	repos := services.ProductSyncRepos{
		Products: h.repo,
		Subjects: h.subjectRepo,
		Niches:   h.nicheRepo,
	}

	result, err := h.service.SyncAllProductsWithExtraction(ctx, repos, services.EksmoSyncOptions{
		PerPage:  perPage,
		MaxPages: maxPages,
		Resume:   resume,
		Reset:    reset,
	})
	if err != nil {
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	return c.JSON(result)
}

func (h *EksmoProductHandler) SyncEksmoAuthors(c *fiber.Ctx) error {
	if h.authorRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "author repository not configured"})
	}

	perPage := parseIntQuery(c, "per_page", 500)
	maxPages := parseIntQuery(c, "max_pages", 0)
	resume := parseBoolQuery(c, "resume", true)
	reset := parseBoolQuery(c, "reset", false)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	result, err := h.service.SyncAllAuthors(ctx, h.authorRepo, services.EksmoSyncOptions{
		PerPage:  perPage,
		MaxPages: maxPages,
		Resume:   resume,
		Reset:    reset,
	})
	if err != nil {
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(result)
}

func (h *EksmoProductHandler) SyncEksmoTags(c *fiber.Ctx) error {
	if h.tagRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "tag repository not configured"})
	}

	perPage := parseIntQuery(c, "per_page", 500)
	maxPages := parseIntQuery(c, "max_pages", 0)
	resume := parseBoolQuery(c, "resume", true)
	reset := parseBoolQuery(c, "reset", false)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	result, err := h.service.SyncAllTags(ctx, h.tagRepo, services.EksmoSyncOptions{
		PerPage:  perPage,
		MaxPages: maxPages,
		Resume:   resume,
		Reset:    reset,
	})
	if err != nil {
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(result)
}

func (h *EksmoProductHandler) SyncEksmoSeries(c *fiber.Ctx) error {
	if h.seriesRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "series repository not configured"})
	}

	perPage := parseIntQuery(c, "per_page", 500)
	maxPages := parseIntQuery(c, "max_pages", 0)
	resume := parseBoolQuery(c, "resume", true)
	reset := parseBoolQuery(c, "reset", false)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	result, err := h.service.SyncAllSeries(ctx, h.seriesRepo, services.EksmoSyncOptions{
		PerPage:  perPage,
		MaxPages: maxPages,
		Resume:   resume,
		Reset:    reset,
	})
	if err != nil {
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(result)
}

func (h *EksmoProductHandler) SyncEksmoPublishers(c *fiber.Ctx) error {
	if h.publisherRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "publisher repository not configured"})
	}

	perPage := parseIntQuery(c, "per_page", 500)
	maxPages := parseIntQuery(c, "max_pages", 0)
	resume := parseBoolQuery(c, "resume", true)
	reset := parseBoolQuery(c, "reset", false)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()

	result, err := h.service.SyncAllPublishers(ctx, h.publisherRepo, services.EksmoSyncOptions{
		PerPage:  perPage,
		MaxPages: maxPages,
		Resume:   resume,
		Reset:    reset,
	})
	if err != nil {
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(result)
}

func (h *EksmoProductHandler) SyncAll(c *fiber.Ctx) error {
	perPage := parseIntQuery(c, "per_page", 500)
	maxPages := parseIntQuery(c, "max_pages", 0)
	resume := parseBoolQuery(c, "resume", true)
	reset := parseBoolQuery(c, "reset", false)

	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	opts := services.EksmoSyncOptions{
		PerPage:  perPage,
		MaxPages: maxPages,
		Resume:   resume,
		Reset:    reset,
	}

	results := fiber.Map{}

	// Sync publishers
	if h.publisherRepo != nil {
		pubResult, err := h.service.SyncAllPublishers(ctx, h.publisherRepo, opts)
		if err != nil {
			results["publishers"] = fiber.Map{"error": err.Error()}
		} else {
			results["publishers"] = pubResult
		}
	}

	// Sync series
	if h.seriesRepo != nil {
		serResult, err := h.service.SyncAllSeries(ctx, h.seriesRepo, opts)
		if err != nil {
			results["series"] = fiber.Map{"error": err.Error()}
		} else {
			results["series"] = serResult
		}
	}

	// Sync authors
	if h.authorRepo != nil {
		authResult, err := h.service.SyncAllAuthors(ctx, h.authorRepo, opts)
		if err != nil {
			results["authors"] = fiber.Map{"error": err.Error()}
		} else {
			results["authors"] = authResult
		}
	}

	// Sync tags
	if h.tagRepo != nil {
		tagResult, err := h.service.SyncAllTags(ctx, h.tagRepo, opts)
		if err != nil {
			results["tags"] = fiber.Map{"error": err.Error()}
		} else {
			results["tags"] = tagResult
		}
	}

	// Sync products
	prodResult, err := h.service.SyncAllProducts(ctx, h.repo, opts)
	if err != nil {
		results["products"] = fiber.Map{"error": err.Error()}
	} else {
		results["products"] = prodResult
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message": "Sync all completed",
		"results": results,
	})
}

type CopyEksmoProductsToMainRequest struct {
	CategoryID  string   `json:"categoryId"`
	ProductIDs  []string `json:"productIds,omitempty"`
	Quantity    int      `json:"quantity,omitempty"`
	Page        int      `json:"page,omitempty"`
	OnlyMissing bool     `json:"onlyMissing,omitempty"`
	AllPages    bool     `json:"allPages,omitempty"`

	Search         string   `json:"search,omitempty"`
	AuthorGUIDs    []string `json:"authorGuids,omitempty"`
	AuthorName     string   `json:"authorName,omitempty"`
	Brand          string   `json:"brand,omitempty"`
	SeriesName     string   `json:"seriesName,omitempty"`
	PublisherName  string   `json:"publisherName,omitempty"`
	AgeRestriction string   `json:"ageRestriction,omitempty"`
	GenreNames     []string `json:"genreNames,omitempty"`
	SubjectGUIDs   []string `json:"subjectGuids,omitempty"`
	NicheGUIDs     []string `json:"nicheGuids,omitempty"`
}

func (h *EksmoProductHandler) CopyEksmoProductsToMain(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	var req CopyEksmoProductsToMainRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	req.CategoryID = strings.TrimSpace(req.CategoryID)
	onlyMissing := req.OnlyMissing || req.AllPages
	if req.CategoryID == "" && !onlyMissing {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "categoryId is required"})
	}

	timeout := 30 * time.Second
	if req.AllPages {
		timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var categoryOID primitive.ObjectID
	var categoryPath []string
	if req.CategoryID != "" {
		if h.categoryLinker == nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "category linker not configured"})
		}
		parsedCategoryID, err := primitive.ObjectIDFromHex(req.CategoryID)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "categoryId must be a valid ObjectID"})
		}
		categoryOID = parsedCategoryID

		if !h.categoryLinker.IsCacheBuilt() {
			if err := h.categoryLinker.BuildCache(ctx); err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to load categories: " + err.Error()})
			}
		}

		categoryPath = h.categoryLinker.GetCategoryPath(categoryOID)
		if len(categoryPath) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "selected main category not found"})
		}
	}

	quantity := req.Quantity
	if quantity <= 0 {
		quantity = 20
	}
	if quantity > 100 {
		quantity = 100
	}

	req.ProductIDs = cleanStringSlice(req.ProductIDs)
	req.AuthorGUIDs = cleanStringSlice(req.AuthorGUIDs)
	req.GenreNames = cleanStringSlice(req.GenreNames)
	req.SubjectGUIDs = cleanStringSlice(req.SubjectGUIDs)
	req.NicheGUIDs = cleanStringSlice(req.NicheGUIDs)

	mode := "group"
	requestedCount := quantity
	scannedCount := 0
	processedCount := 0
	upserted := 0
	modified := 0
	skipped := 0
	totalMatched := int64(0)
	idsTrueAll := make(map[primitive.ObjectID]struct{})
	idsFalseAll := make(map[primitive.ObjectID]struct{})

	processBatch := func(batch []models.EksmoProduct) error {
		if len(batch) == 0 {
			return nil
		}
		scannedCount += len(batch)

		targetProducts := batch
		if onlyMissing {
			flags, ferr := h.mainProductRepo.ExistsForEksmoProducts(ctx, batch)
			if ferr != nil {
				return ferr
			}
			if len(flags) != len(batch) {
				return errors.New("failed to evaluate existing products")
			}

			missing := make([]models.EksmoProduct, 0, len(batch))
			for i, product := range batch {
				if flags[i] {
					continue
				}
				missing = append(missing, product)
			}
			targetProducts = missing
		}

		processedCount += len(targetProducts)
		if len(targetProducts) > 0 {
			batchUpserted, batchModified, batchSkipped, uerr := h.mainProductRepo.UpsertFromEksmoProducts(ctx, targetProducts, categoryOID, categoryPath)
			if uerr != nil {
				return uerr
			}
			upserted += batchUpserted
			modified += batchModified
			skipped += batchSkipped
		}

		flags, ferr := h.mainProductRepo.ExistsForEksmoProducts(ctx, batch)
		if ferr == nil && len(flags) == len(batch) {
			idsTrue, idsFalse := splitProductIDsByFlags(batch, flags)
			if req.AllPages {
				for _, id := range idsTrue {
					idsTrueAll[id] = struct{}{}
					delete(idsFalseAll, id)
				}
				for _, id := range idsFalse {
					if _, exists := idsTrueAll[id]; exists {
						continue
					}
					idsFalseAll[id] = struct{}{}
				}
			} else {
				_ = h.repo.SetInMainProductsByIDs(ctx, idsTrue, true)
				_ = h.repo.SetInMainProductsByIDs(ctx, idsFalse, false)
			}
		}

		return nil
	}

	if len(req.ProductIDs) > 0 {
		mode = "selected"
		if len(req.ProductIDs) > 100 {
			req.ProductIDs = req.ProductIDs[:100]
		}
		requestedCount = len(req.ProductIDs)

		ids := make([]primitive.ObjectID, 0, len(req.ProductIDs))
		for _, idStr := range req.ProductIDs {
			oid, err := primitive.ObjectIDFromHex(idStr)
			if err == nil {
				ids = append(ids, oid)
			}
		}

		if len(ids) == 0 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "productIds must contain at least one valid ObjectID"})
		}

		products, err := h.repo.ListByIDs(ctx, ids)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		totalMatched = int64(len(products))
		if len(products) == 0 {
			return c.JSON(fiber.Map{
				"message":      "No products matched the request",
				"mode":         mode,
				"categoryId":   req.CategoryID,
				"categoryPath": categoryPath,
				"requested":    requestedCount,
				"processed":    0,
				"scanned":      0,
				"copied":       0,
				"onlyMissing":  onlyMissing,
				"allPages":     req.AllPages,
			})
		}
		if err := processBatch(products); err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
	} else {
		page := req.Page
		if page <= 0 {
			page = 1
		}

		params := repository.ProductFilterParams{
			Page:         int64(page),
			Limit:        int64(quantity),
			Search:       strings.TrimSpace(req.Search),
			AuthorGUIDs:  req.AuthorGUIDs,
			GenreNames:   req.GenreNames,
			SubjectGUIDs: req.SubjectGUIDs,
		}
		params.AuthorName, params.AuthorNames = parseSingleOrCSV(req.AuthorName)
		params.Brand, params.Brands = parseSingleOrCSV(req.Brand)
		params.SeriesName, params.SeriesNames = parseSingleOrCSV(req.SeriesName)
		params.PublisherName, params.PublisherNames = parseSingleOrCSV(req.PublisherName)
		params.AgeRestriction, params.AgeRestrictions = parseSingleOrCSV(req.AgeRestriction)

		if len(req.NicheGUIDs) > 0 {
			nicheGUIDs := []string{}
			for _, nicheGUID := range req.NicheGUIDs {
				if h.treeBuilder != nil {
					desc, derr := h.treeBuilder.GetAllDescendantNicheGUIDs(ctx, nicheGUID)
					if derr == nil && len(desc) > 0 {
						nicheGUIDs = append(nicheGUIDs, desc...)
						continue
					}
				}
				nicheGUIDs = append(nicheGUIDs, nicheGUID)
			}
			params.NicheGUIDs = uniqueStrings(nicheGUIDs)
		}

		if req.AllPages {
			mode = "all"
			params.Page = 1
			for {
				products, total, err := h.repo.ListWithFilters(ctx, params)
				if err != nil {
					return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
				}
				if params.Page == 1 {
					totalMatched = total
					requestedCount = int(total)
				}
				if len(products) == 0 {
					break
				}
				if len(products) > quantity {
					products = products[:quantity]
				}
				if err := processBatch(products); err != nil {
					return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
				}
				if params.Page*params.Limit >= total {
					break
				}
				params.Page++
			}
		} else {
			products, total, err := h.repo.ListWithFilters(ctx, params)
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
			}
			totalMatched = total
			if len(products) > quantity {
				products = products[:quantity]
			}
			if len(products) == 0 {
				return c.JSON(fiber.Map{
					"message":      "No products matched the request",
					"mode":         mode,
					"categoryId":   req.CategoryID,
					"categoryPath": categoryPath,
					"requested":    requestedCount,
					"processed":    0,
					"scanned":      0,
					"copied":       0,
					"onlyMissing":  onlyMissing,
					"allPages":     req.AllPages,
				})
			}
			if err := processBatch(products); err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
			}
		}
	}

	if req.AllPages {
		idsTrue := mapObjectIDKeys(idsTrueAll)
		idsFalse := mapObjectIDKeys(idsFalseAll)
		if len(idsTrue) > 0 {
			_ = h.repo.SetInMainProductsByIDs(ctx, idsTrue, true)
		}
		if len(idsFalse) > 0 {
			_ = h.repo.SetInMainProductsByIDs(ctx, idsFalse, false)
		}
	}
	h.invalidateProductCaches()

	responseMessage := "Copied Eksmo products to main_products"
	if onlyMissing {
		responseMessage = "Copied missing Eksmo products to main_products"
	}

	return c.JSON(fiber.Map{
		"message":      responseMessage,
		"mode":         mode,
		"categoryId":   req.CategoryID,
		"categoryPath": categoryPath,
		"requested":    requestedCount,
		"matched":      totalMatched,
		"scanned":      scannedCount,
		"processed":    processedCount,
		"copied":       upserted + modified,
		"upserted":     upserted,
		"modified":     modified,
		"skipped":      skipped,
		"maxQuantity":  100,
		"onlyMissing":  onlyMissing,
		"allPages":     req.AllPages,
	})
}

func (h *EksmoProductHandler) LinkProductCategories(c *fiber.Ctx) error {
	if h.categoryLinker == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "category linker not configured"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	// Build category cache
	if err := h.categoryLinker.BuildCache(ctx); err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to build category cache: " + err.Error()})
	}

	// Link all products
	linked, err := h.repo.LinkAllProductsWithCategories(ctx, h.categoryLinker.LinkProduct)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message":       "Products linked with categories",
		"linked":        linked,
		"categoryStats": h.categoryLinker.GetStats(),
	})
}

func (h *EksmoProductHandler) GetMainProducts(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}
	searchQuery := strings.TrimSpace(c.Query("search"))
	if searchQuery == "" {
		if served, err := h.tryServeCachedJSON(c, cacheNamespaceMainProducts); served || err != nil {
			return err
		}
	}

	params := repository.MainProductFilterParams{
		Page:            int64(parseIntQuery(c, "page", 1)),
		Limit:           int64(parseIntQuery(c, "limit", 20)),
		Search:          searchQuery,
		WithoutCategory: parseBoolQuery(c, "withoutCategory", false),
		WithoutISBN:     parseBoolQuery(c, "withoutIsbn", false),
	}

	if params.Limit > 200 {
		params.Limit = 200
	}

	categoryIDs := parseObjectIDsCSV(c.Query("categoryIds"))
	if categoryIDStr := strings.TrimSpace(c.Query("categoryId")); categoryIDStr != "" {
		if oid, err := primitive.ObjectIDFromHex(categoryIDStr); err == nil {
			categoryIDs = append(categoryIDs, oid)
		}
	}
	params.CategoryIDs = h.expandCategoryFilters(categoryIDs)
	if len(params.CategoryIDs) == 0 && len(categoryIDs) == 1 {
		params.CategoryID = categoryIDs[0]
	}
	sourceCategoryPaths, otherCategoryPaths, sourceDomains, includeWithoutCategory, includeEksmo := parseMainProductSourceCategoryFilter(c.Query("sourceCategoryKeys"))
	params.SourceCategoryPaths = sourceCategoryPaths
	params.OtherCategoryPaths = otherCategoryPaths
	params.SourceDomains = sourceDomains
	if includeWithoutCategory {
		params.WithoutCategory = true
	}
	if includeEksmo {
		params.IncludeEksmoSources = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	products, total, err := h.mainProductRepo.ListWithFilters(ctx, params)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if params.Limit > 0 {
		totalPages = (total + params.Limit - 1) / params.Limit
	}

	payload := fiber.Map{
		"collection": "main_products",
		"data":       products,
		"pagination": fiber.Map{
			"page":       params.Page,
			"limit":      params.Limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	}
	return h.respondJSONWithCache(c, cacheNamespaceMainProducts, payload)
}

func (h *EksmoProductHandler) SyncMainProductsFromBillz(c *fiber.Ctx) error {
	if h.billzSync == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "billz sync service is not configured"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	result, err := h.billzSync.SyncNow(ctx)
	if err != nil {
		if errors.Is(err, services.ErrBillzSyncRunning) {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "billz sync is already running"})
		}
		return c.Status(fiber.StatusBadGateway).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message":         "Billz sync completed",
		"fetchedProducts": result.FetchedProducts,
		"uniqueBarcodes":  result.UniqueBarcodes,
		"candidates":      result.Candidates,
		"matched":         result.Matched,
		"updated":         result.Updated,
	})
}

type DeleteMainProductsRequest struct {
	ProductIDs         []string `json:"productIds"`
	ExcludeProductIDs  []string `json:"excludeProductIds"`
	Search             string   `json:"search"`
	SourceCategoryKeys string   `json:"sourceCategoryKeys"`
	WithoutCategory    bool     `json:"withoutCategory"`
	WithoutISBN        bool     `json:"withoutIsbn"`
	ApplyToFiltered    bool     `json:"applyToFiltered"`
}

type LinkMainProductsCategoryRequest struct {
	ProductIDs         []string `json:"productIds"`
	ExcludeProductIDs  []string `json:"excludeProductIds"`
	CategoryID         string   `json:"categoryId"`
	Search             string   `json:"search"`
	SourceCategoryKeys string   `json:"sourceCategoryKeys"`
	WithoutCategory    bool     `json:"withoutCategory"`
	WithoutISBN        bool     `json:"withoutIsbn"`
	ApplyToFiltered    bool     `json:"applyToFiltered"`
}

type UnlinkMainProductsCategoryRequest struct {
	ProductIDs         []string `json:"productIds"`
	ExcludeProductIDs  []string `json:"excludeProductIds"`
	Search             string   `json:"search"`
	SourceCategoryKeys string   `json:"sourceCategoryKeys"`
	WithoutCategory    bool     `json:"withoutCategory"`
	WithoutISBN        bool     `json:"withoutIsbn"`
	ApplyToFiltered    bool     `json:"applyToFiltered"`
}

type MainProductSourceCategoryNode struct {
	ID       string                           `json:"id"`
	Name     string                           `json:"name"`
	Path     []string                         `json:"path"`
	Children []*MainProductSourceCategoryNode `json:"children,omitempty"`
}

const (
	mainProductSourceCategoryKeyPathPrefix      = "path:"
	mainProductSourceCategoryKeyOtherPathPrefix = "otherPath:"
	mainProductSourceCategoryKeyDomainPrefix    = "domain:"
	mainProductSourceCategoryKeyOther           = "other"
	mainProductSourceCategoryKeyEksmo           = "group:eksmo"
	mainProductSourceCategoryKeyDomains         = "group:domains"
)

type DeleteEksmoProductsRequest struct {
	ProductIDs []string `json:"productIds"`
}

func (h *EksmoProductHandler) DeleteEksmoProducts(c *fiber.Ctx) error {
	if h.repo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "eksmo product repository not configured"})
	}

	var req DeleteEksmoProductsRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	req.ProductIDs = cleanStringSlice(req.ProductIDs)
	if len(req.ProductIDs) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "productIds must contain at least one id"})
	}
	if len(req.ProductIDs) > 100 {
		req.ProductIDs = req.ProductIDs[:100]
	}

	ids := make([]primitive.ObjectID, 0, len(req.ProductIDs))
	seen := make(map[primitive.ObjectID]struct{}, len(req.ProductIDs))
	invalid := 0
	for _, idStr := range req.ProductIDs {
		oid, err := primitive.ObjectIDFromHex(idStr)
		if err != nil {
			invalid++
			continue
		}
		if _, exists := seen[oid]; exists {
			continue
		}
		seen[oid] = struct{}{}
		ids = append(ids, oid)
	}
	if len(ids) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "productIds must contain at least one valid ObjectID"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	deletedCount, err := h.repo.DeleteByIDs(ctx, ids)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	deleted := int(deletedCount)
	notFound := len(ids) - deleted
	if notFound < 0 {
		notFound = 0
	}

	return c.JSON(fiber.Map{
		"message":   "Products removed from eksmo_products",
		"requested": len(req.ProductIDs),
		"valid":     len(ids),
		"deleted":   deleted,
		"notFound":  notFound,
		"invalid":   invalid,
	})
}

func (h *EksmoProductHandler) DeleteEksmoProduct(c *fiber.Ctx) error {
	if h.repo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "eksmo product repository not configured"})
	}

	idStr := strings.TrimSpace(c.Params("id"))
	oid, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid product id"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deleted, err := h.repo.DeleteByID(ctx, oid)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if !deleted {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "product not found"})
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message": "Product removed from eksmo_products",
		"id":      idStr,
	})
}

func (h *EksmoProductHandler) DeleteMainProducts(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	var req DeleteMainProductsRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if req.ApplyToFiltered {
		params := repository.MainProductFilterParams{
			Search:          strings.TrimSpace(req.Search),
			WithoutCategory: req.WithoutCategory,
			WithoutISBN:     req.WithoutISBN,
			ExcludeIDs:      parseObjectIDsSlice(req.ExcludeProductIDs),
		}
		sourceCategoryPaths, otherCategoryPaths, sourceDomains, includeWithoutCategory, includeEksmo := parseMainProductSourceCategoryFilter(req.SourceCategoryKeys)
		params.SourceCategoryPaths = sourceCategoryPaths
		params.OtherCategoryPaths = otherCategoryPaths
		params.SourceDomains = sourceDomains
		if includeWithoutCategory {
			params.WithoutCategory = true
		}
		if includeEksmo {
			params.IncludeEksmoSources = true
		}

		deletedProducts, err := h.mainProductRepo.DeleteByFilter(ctx, params)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		h.refreshEksmoMainFlagsAfterDelete(ctx, deletedProducts)
		h.invalidateProductCaches()

		return c.JSON(fiber.Map{
			"message": "Filtered products removed from main_products",
			"mode":    "filtered",
			"deleted": len(deletedProducts),
		})
	}

	req.ProductIDs = cleanStringSlice(req.ProductIDs)
	if len(req.ProductIDs) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "productIds must contain at least one id"})
	}
	if len(req.ProductIDs) > 100 {
		req.ProductIDs = req.ProductIDs[:100]
	}

	ids := make([]primitive.ObjectID, 0, len(req.ProductIDs))
	seen := make(map[primitive.ObjectID]struct{}, len(req.ProductIDs))
	invalid := 0
	for _, idStr := range req.ProductIDs {
		oid, err := primitive.ObjectIDFromHex(idStr)
		if err != nil {
			invalid++
			continue
		}
		if _, exists := seen[oid]; exists {
			continue
		}
		seen[oid] = struct{}{}
		ids = append(ids, oid)
	}
	if len(ids) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "productIds must contain at least one valid ObjectID"})
	}

	deletedProducts, err := h.mainProductRepo.DeleteByIDs(ctx, ids)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	h.refreshEksmoMainFlagsAfterDelete(ctx, deletedProducts)
	h.invalidateProductCaches()

	deleted := len(deletedProducts)
	notFound := len(ids) - deleted
	if notFound < 0 {
		notFound = 0
	}

	return c.JSON(fiber.Map{
		"message":   "Products removed from main_products",
		"requested": len(req.ProductIDs),
		"valid":     len(ids),
		"deleted":   deleted,
		"notFound":  notFound,
		"invalid":   invalid,
	})
}

func (h *EksmoProductHandler) DeleteMainProduct(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	idStr := strings.TrimSpace(c.Params("id"))
	oid, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid product id"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	deletedProduct, deleted, err := h.mainProductRepo.DeleteByID(ctx, oid)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if !deleted {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "product not found"})
	}

	if deletedProduct != nil {
		h.refreshEksmoMainFlagsAfterDelete(ctx, []models.MainProduct{*deletedProduct})
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message": "Product removed from main_products",
		"id":      idStr,
	})
}

func (h *EksmoProductHandler) GetMainProductsSourceCategories(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	paths, err := h.mainProductRepo.ListSourceCategoryPaths(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	otherPaths, err := h.mainProductRepo.ListUncategorizedCategoryHints(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	visiblePaths := make([][]string, 0, len(paths))
	for _, path := range paths {
		if shouldHideMainSourceCategoryPath(path) {
			continue
		}
		visiblePaths = append(visiblePaths, path)
	}
	nodes := buildMainProductSourceCategoryTree(visiblePaths, mainProductSourceCategoryKeyPathPrefix)
	otherChildren := buildMainProductSourceCategoryTree(otherPaths, mainProductSourceCategoryKeyOtherPathPrefix)
	otherNode := &MainProductSourceCategoryNode{
		ID:       mainProductSourceCategoryKeyOther,
		Name:     "Other",
		Path:     []string{"Other"},
		Children: otherChildren,
	}
	grouped := make([]*MainProductSourceCategoryNode, 0, len(nodes)+2)
	grouped = append(grouped, otherNode)
	grouped = append(grouped, nodes...)

	return c.JSON(fiber.Map{
		"collection": "main_products_source_categories",
		"data":       grouped,
	})
}

func (h *EksmoProductHandler) LinkMainProductsCategory(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	var req LinkMainProductsCategoryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	categoryIDStr := strings.TrimSpace(req.CategoryID)
	if categoryIDStr == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "categoryId is required"})
	}
	categoryOID, err := primitive.ObjectIDFromHex(categoryIDStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "categoryId must be a valid ObjectID"})
	}

	var categoryPath []string
	if h.categoryLinker != nil {
		cacheCtx, cacheCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = h.categoryLinker.BuildCache(cacheCtx)
		cacheCancel()
		categoryPath = h.categoryLinker.GetCategoryPath(categoryOID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if req.ApplyToFiltered {
		params := repository.MainProductFilterParams{
			Search:          strings.TrimSpace(req.Search),
			WithoutCategory: req.WithoutCategory,
			WithoutISBN:     req.WithoutISBN,
			ExcludeIDs:      parseObjectIDsSlice(req.ExcludeProductIDs),
		}
		sourceCategoryPaths, otherCategoryPaths, sourceDomains, includeWithoutCategory, includeEksmo := parseMainProductSourceCategoryFilter(req.SourceCategoryKeys)
		params.SourceCategoryPaths = sourceCategoryPaths
		params.OtherCategoryPaths = otherCategoryPaths
		params.SourceDomains = sourceDomains
		if includeWithoutCategory {
			params.WithoutCategory = true
		}
		if includeEksmo {
			params.IncludeEksmoSources = true
		}

		matched, modified, err := h.mainProductRepo.AssignCategoryByFilter(ctx, params, categoryOID, categoryPath)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		h.invalidateProductCaches()

		return c.JSON(fiber.Map{
			"message":      "Filtered main products linked to category",
			"mode":         "filtered",
			"linked":       matched,
			"modified":     modified,
			"categoryId":   categoryIDStr,
			"categoryPath": categoryPath,
		})
	}

	req.ProductIDs = cleanStringSlice(req.ProductIDs)
	if len(req.ProductIDs) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "productIds must contain at least one id"})
	}
	if len(req.ProductIDs) > 100 {
		req.ProductIDs = req.ProductIDs[:100]
	}

	ids := make([]primitive.ObjectID, 0, len(req.ProductIDs))
	invalid := 0
	for _, rawID := range req.ProductIDs {
		oid, parseErr := primitive.ObjectIDFromHex(rawID)
		if parseErr != nil {
			invalid++
			continue
		}
		ids = append(ids, oid)
	}
	if len(ids) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no valid product ids provided", "invalid": invalid})
	}

	matched, modified, err := h.mainProductRepo.AssignCategoryByIDs(ctx, ids, categoryOID, categoryPath)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	notFound := len(ids) - int(matched)
	if notFound < 0 {
		notFound = 0
	}

	return c.JSON(fiber.Map{
		"message":      "Main products linked to category",
		"mode":         "selected",
		"requested":    len(req.ProductIDs),
		"valid":        len(ids),
		"linked":       matched,
		"modified":     modified,
		"notFound":     notFound,
		"invalid":      invalid,
		"categoryId":   categoryIDStr,
		"categoryPath": categoryPath,
	})
}

func (h *EksmoProductHandler) UnlinkMainProductsCategory(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	var req UnlinkMainProductsCategoryRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if req.ApplyToFiltered {
		params := repository.MainProductFilterParams{
			Search:          strings.TrimSpace(req.Search),
			WithoutCategory: req.WithoutCategory,
			WithoutISBN:     req.WithoutISBN,
			ExcludeIDs:      parseObjectIDsSlice(req.ExcludeProductIDs),
		}
		sourceCategoryPaths, otherCategoryPaths, sourceDomains, includeWithoutCategory, includeEksmo := parseMainProductSourceCategoryFilter(req.SourceCategoryKeys)
		params.SourceCategoryPaths = sourceCategoryPaths
		params.OtherCategoryPaths = otherCategoryPaths
		params.SourceDomains = sourceDomains
		if includeWithoutCategory {
			params.WithoutCategory = true
		}
		if includeEksmo {
			params.IncludeEksmoSources = true
		}

		matched, modified, err := h.mainProductRepo.RemoveCategoryByFilter(ctx, params)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		h.invalidateProductCaches()

		return c.JSON(fiber.Map{
			"message":  "Filtered main products unlinked from category",
			"mode":     "filtered",
			"unlinked": matched,
			"modified": modified,
		})
	}

	req.ProductIDs = cleanStringSlice(req.ProductIDs)
	if len(req.ProductIDs) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "productIds must contain at least one id"})
	}
	if len(req.ProductIDs) > 100 {
		req.ProductIDs = req.ProductIDs[:100]
	}

	ids := make([]primitive.ObjectID, 0, len(req.ProductIDs))
	invalid := 0
	for _, rawID := range req.ProductIDs {
		oid, parseErr := primitive.ObjectIDFromHex(rawID)
		if parseErr != nil {
			invalid++
			continue
		}
		ids = append(ids, oid)
	}
	if len(ids) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no valid product ids provided", "invalid": invalid})
	}

	matched, modified, err := h.mainProductRepo.RemoveCategoryByIDs(ctx, ids)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	notFound := len(ids) - int(matched)
	if notFound < 0 {
		notFound = 0
	}

	return c.JSON(fiber.Map{
		"message":   "Main products unlinked from category",
		"mode":      "selected",
		"requested": len(req.ProductIDs),
		"valid":     len(ids),
		"unlinked":  matched,
		"modified":  modified,
		"notFound":  notFound,
		"invalid":   invalid,
	})
}

func (h *EksmoProductHandler) RemoveMainProductCategory(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	idStr := strings.TrimSpace(c.Params("id"))
	oid, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid product id"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	updated, err := h.mainProductRepo.RemoveCategoryByID(ctx, oid)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	if !updated {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "product not found"})
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message": "Category removed from product",
		"id":      idStr,
	})
}

func (h *EksmoProductHandler) refreshEksmoMainFlagsAfterDelete(ctx context.Context, products []models.MainProduct) {
	if len(products) == 0 || h.mainProductRepo == nil || h.repo == nil {
		return
	}

	seen := make(map[string]struct{}, len(products))
	for _, product := range products {
		key, guidNom, guid, nomcode := mainProductSourceLookup(product)
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		stillExists, err := h.mainProductRepo.ExistsBySource(ctx, guidNom, guid, nomcode)
		if err == nil {
			_ = h.repo.SetInMainProductsBySource(ctx, guidNom, guid, nomcode, stillExists)
		}
	}
}

func mainProductSourceLookup(product models.MainProduct) (key string, guidNom string, guid string, nomcode string) {
	if value := strings.TrimSpace(product.SourceGUIDNOM); value != "" {
		return "guidNom:" + value, value, "", ""
	}
	if value := strings.TrimSpace(product.SourceGUID); value != "" {
		return "guid:" + value, "", value, ""
	}
	if value := strings.TrimSpace(product.SourceNomCode); value != "" {
		return "nomcode:" + value, "", "", value
	}
	return "", "", "", ""
}

func normalizeDuplicateCode(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(trimmed))
	for _, r := range strings.ToUpper(trimmed) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

func (h *EksmoProductHandler) GetEksmoProductsDuplicates(c *fiber.Ctx) error {
	if served, err := h.tryServeCachedJSON(c, cacheNamespaceEksmoProductsDuplicates); served || err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	rows, err := h.repo.ListDuplicateScanRecords(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	valueToProductIDs := make(map[string]map[string]struct{}, len(rows))
	idByHex := make(map[string]primitive.ObjectID, len(rows))
	scanned := 0

	for _, row := range rows {
		if row.ID.IsZero() {
			continue
		}
		productID := row.ID.Hex()
		idByHex[productID] = row.ID
		scanned++

		// Per-product dedupe: same normalized value should be counted once.
		normalizedValues := map[string]struct{}{}
		for _, raw := range []string{row.ISBN, row.NomCode} {
			normalized := normalizeDuplicateCode(raw)
			if normalized != "" {
				normalizedValues[normalized] = struct{}{}
			}
		}

		for normalized := range normalizedValues {
			if _, exists := valueToProductIDs[normalized]; !exists {
				valueToProductIDs[normalized] = map[string]struct{}{}
			}
			valueToProductIDs[normalized][productID] = struct{}{}
		}
	}

	duplicateProductIDSet := map[string]struct{}{}
	duplicateGroups := 0
	for _, productSet := range valueToProductIDs {
		if len(productSet) <= 1 {
			continue
		}
		duplicateGroups++
		for productID := range productSet {
			duplicateProductIDSet[productID] = struct{}{}
		}
	}

	duplicateIDs := make([]primitive.ObjectID, 0, len(duplicateProductIDSet))
	for productID := range duplicateProductIDSet {
		if oid, exists := idByHex[productID]; exists && !oid.IsZero() {
			duplicateIDs = append(duplicateIDs, oid)
		}
	}
	sort.Slice(duplicateIDs, func(i, j int) bool {
		return duplicateIDs[i].Hex() < duplicateIDs[j].Hex()
	})

	products, err := h.repo.ListByIDsLite(ctx, duplicateIDs)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	payload := fiber.Map{
		"collection": "eksmo_products_duplicates",
		"data":       products,
		"summary": fiber.Map{
			"scanned":           scanned,
			"duplicateGroups":   duplicateGroups,
			"duplicateProducts": len(products),
		},
	}
	return h.respondJSONWithCache(c, cacheNamespaceEksmoProductsDuplicates, payload)
}

func (h *EksmoProductHandler) GetEksmoProducts(c *fiber.Ctx) error {
	if served, err := h.tryServeCachedJSON(c, cacheNamespaceEksmoProducts); served || err != nil {
		return err
	}

	params := repository.ProductFilterParams{
		Page:    int64(parseIntQuery(c, "page", 1)),
		Limit:   int64(parseIntQuery(c, "limit", 20)),
		Search:  c.Query("search"),
		TagName: c.Query("tagName"),
		Subject: c.Query("subject"),
	}

	params.AuthorName, params.AuthorNames = parseSingleOrCSV(c.Query("authorName"))
	params.Brand, params.Brands = parseSingleOrCSV(c.Query("brand"))
	params.SeriesName, params.SeriesNames = parseSingleOrCSV(c.Query("serie"))
	params.PublisherName, params.PublisherNames = parseSingleOrCSV(c.Query("publisher"))
	params.AgeRestriction, params.AgeRestrictions = parseSingleOrCSV(c.Query("age"))

	if params.Limit > 200 {
		params.Limit = 200
	}

	// Parse category IDs for nested filtering (supports legacy categoryId too)
	categoryIDs := parseObjectIDsCSV(c.Query("categoryIds"))
	if categoryIDStr := strings.TrimSpace(c.Query("categoryId")); categoryIDStr != "" {
		if oid, err := primitive.ObjectIDFromHex(categoryIDStr); err == nil {
			categoryIDs = append(categoryIDs, oid)
		}
	}
	params.CategoryIDs = h.expandCategoryFilters(categoryIDs)
	if len(params.CategoryIDs) == 0 && len(categoryIDs) == 1 {
		params.CategoryID = categoryIDs[0]
	}

	// Parse author GUIDs (comma-separated)
	if authorGuids := c.Query("authorGuids"); authorGuids != "" {
		params.AuthorGUIDs = splitAndTrim(authorGuids, ",")
	}

	// Parse tag GUIDs (comma-separated)
	if tagGuids := c.Query("tagGuids"); tagGuids != "" {
		params.TagGUIDs = splitAndTrim(tagGuids, ",")
	}

	// Parse genre names (comma-separated)
	if genres := c.Query("genres"); genres != "" {
		params.GenreNames = cleanStringSlice(splitAndTrim(genres, ","))
	}

	// Parse series GUID
	params.SeriesGUID = c.Query("seriesGuid")

	// Parse publisher GUID
	params.PublisherGUID = c.Query("publisherGuid")

	// Parse subject GUIDs (comma-separated for multiple selection)
	if subjectGuid := c.Query("subjectGuid"); subjectGuid != "" {
		params.SubjectGUIDs = splitAndTrim(subjectGuid, ",")
	}

	// Parse niche GUIDs (comma-separated) - for each, get all descendant niche GUIDs
	if nicheGuid := c.Query("nicheGuid"); nicheGuid != "" {
		inputNiches := splitAndTrim(nicheGuid, ",")
		allNicheGUIDs := []string{}

		for _, nGuid := range inputNiches {
			if h.treeBuilder != nil {
				// Get all descendant niche GUIDs including the niche itself
				nicheGUIDs, err := h.treeBuilder.GetAllDescendantNicheGUIDs(context.Background(), nGuid)
				if err == nil && len(nicheGUIDs) > 0 {
					allNicheGUIDs = append(allNicheGUIDs, nicheGUIDs...)
				}
			} else {
				allNicheGUIDs = append(allNicheGUIDs, nGuid)
			}
		}

		if len(allNicheGUIDs) > 0 {
			params.NicheGUIDs = allNicheGUIDs
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	products, total, err := h.repo.ListWithFilters(ctx, params)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if params.Limit > 0 {
		totalPages = (total + params.Limit - 1) / params.Limit
	}

	payload := fiber.Map{
		"collection": "eksmo_products",
		"data":       products,
		"pagination": fiber.Map{
			"page":       params.Page,
			"limit":      params.Limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	}
	return h.respondJSONWithCache(c, cacheNamespaceEksmoProducts, payload)
}

func (h *EksmoProductHandler) GetEksmoProductsMeta(c *fiber.Ctx) error {
	if served, err := h.tryServeCachedJSON(c, cacheNamespaceEksmoProductsMeta); served || err != nil {
		return err
	}

	limit := parseIntQuery(c, "limit", 1000)
	if limit < 0 {
		limit = 1000
	}
	if limit > 5000 {
		limit = 5000
	}

	expanded := parseBoolQuery(c, "expanded", true)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var err error
	var meta interface{}

	if expanded {
		meta, err = h.repo.MetaExpanded(ctx, limit)
	} else {
		meta, err = h.repo.Meta(ctx, limit)
	}

	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	payload := fiber.Map{
		"collection": "eksmo_products",
		"data":       meta,
	}
	return h.respondJSONWithCache(c, cacheNamespaceEksmoProductsMeta, payload)
}

func (h *EksmoProductHandler) GetEksmoAuthors(c *fiber.Ctx) error {
	if h.authorRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "author repository not configured"})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	search := c.Query("search")
	writerOnly := parseBoolQuery(c, "writerOnly", false)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	authors, total, err := h.authorRepo.List(ctx, page, limit, search, writerOnly)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}

	return c.JSON(fiber.Map{
		"collection": "eksmo_authors",
		"data":       authors,
		"pagination": fiber.Map{
			"page":       page,
			"limit":      limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	})
}

func (h *EksmoProductHandler) GetEksmoTags(c *fiber.Ctx) error {
	if h.tagRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "tag repository not configured"})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	search := c.Query("search")
	activeOnly := parseBoolQuery(c, "activeOnly", true)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tags, total, err := h.tagRepo.List(ctx, page, limit, search, activeOnly)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}

	return c.JSON(fiber.Map{
		"collection": "eksmo_tags",
		"data":       tags,
		"pagination": fiber.Map{
			"page":       page,
			"limit":      limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	})
}

func (h *EksmoProductHandler) GetEksmoSeries(c *fiber.Ctx) error {
	if h.seriesRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "series repository not configured"})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	search := c.Query("search")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seriesList, total, err := h.seriesRepo.List(ctx, page, limit, search)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}

	return c.JSON(fiber.Map{
		"collection": "eksmo_series",
		"data":       seriesList,
		"pagination": fiber.Map{
			"page":       page,
			"limit":      limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	})
}

func (h *EksmoProductHandler) GetEksmoPublishers(c *fiber.Ctx) error {
	if h.publisherRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "publisher repository not configured"})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	search := c.Query("search")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	publishers, total, err := h.publisherRepo.List(ctx, page, limit, search)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}

	return c.JSON(fiber.Map{
		"collection": "eksmo_publishers",
		"data":       publishers,
		"pagination": fiber.Map{
			"page":       page,
			"limit":      limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	})
}

func (h *EksmoProductHandler) GetEksmoSubjects(c *fiber.Ctx) error {
	if h.subjectRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "subject repository not configured"})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	search := c.Query("search")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	subjects, total, err := h.subjectRepo.List(ctx, page, limit, search)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}

	return c.JSON(fiber.Map{
		"collection": "eksmo_subjects",
		"data":       subjects,
		"pagination": fiber.Map{
			"page":       page,
			"limit":      limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	})
}

func (h *EksmoProductHandler) GetEksmoNiches(c *fiber.Ctx) error {
	if h.nicheRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "niche repository not configured"})
	}

	page := int64(parseIntQuery(c, "page", 1))
	limit := int64(parseIntQuery(c, "limit", 50))
	search := c.Query("search")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	niches, total, err := h.nicheRepo.List(ctx, page, limit, search)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	totalPages := int64(0)
	if limit > 0 {
		totalPages = (total + limit - 1) / limit
	}

	return c.JSON(fiber.Map{
		"collection": "eksmo_niches",
		"data":       niches,
		"pagination": fiber.Map{
			"page":       page,
			"limit":      limit,
			"totalItems": total,
			"totalPages": totalPages,
		},
	})
}

func (h *EksmoProductHandler) GetEksmoNichesTree(c *fiber.Ctx) error {
	if h.treeBuilder == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "tree builder not configured"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tree, err := h.treeBuilder.BuildTree(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	return c.JSON(fiber.Map{
		"collection": "eksmo_niches_tree",
		"data":       tree,
	})
}

func parseIntQuery(c *fiber.Ctx, key string, fallback int) int {
	value := c.Query(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	if parsed < 0 {
		return fallback
	}
	return parsed
}

func parseBoolQuery(c *fiber.Ctx, key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(c.Query(key)))
	if value == "" {
		return fallback
	}
	switch value {
	case "1", "true", "yes":
		return true
	case "0", "false", "no":
		return false
	default:
		return fallback
	}
}

func splitAndTrim(s string, sep string) []string {
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

func parseSingleOrCSV(raw string) (single string, multiple []string) {
	values := cleanStringSlice(splitAndTrim(strings.TrimSpace(raw), ","))
	if len(values) > 1 {
		return "", values
	}
	if len(values) == 1 {
		return values[0], nil
	}
	return "", nil
}

func parseObjectIDsCSV(raw string) []primitive.ObjectID {
	values := splitAndTrim(raw, ",")
	if len(values) == 0 {
		return nil
	}

	result := make([]primitive.ObjectID, 0, len(values))
	seen := make(map[primitive.ObjectID]struct{}, len(values))
	for _, value := range values {
		oid, err := primitive.ObjectIDFromHex(value)
		if err != nil {
			continue
		}
		if _, exists := seen[oid]; exists {
			continue
		}
		seen[oid] = struct{}{}
		result = append(result, oid)
	}
	return result
}

func parseObjectIDsSlice(values []string) []primitive.ObjectID {
	cleaned := cleanStringSlice(values)
	if len(cleaned) == 0 {
		return nil
	}

	result := make([]primitive.ObjectID, 0, len(cleaned))
	seen := make(map[primitive.ObjectID]struct{}, len(cleaned))
	for _, value := range cleaned {
		oid, err := primitive.ObjectIDFromHex(value)
		if err != nil {
			continue
		}
		if _, exists := seen[oid]; exists {
			continue
		}
		seen[oid] = struct{}{}
		result = append(result, oid)
	}
	return result
}

func parseMainProductSourceCategoryFilter(raw string) (sourcePaths [][]string, otherPaths [][]string, sourceDomains []string, includeWithoutCategory bool, includeEksmo bool) {
	values := cleanStringSlice(splitAndTrim(raw, ","))
	if len(values) == 0 {
		return nil, nil, nil, false, false
	}

	seenSource := make(map[string]struct{}, len(values))
	seenOther := make(map[string]struct{}, len(values))
	seenDomains := make(map[string]struct{}, len(values))
	sourceResult := make([][]string, 0, len(values))
	otherResult := make([][]string, 0, len(values))
	sourceDomainResult := make([]string, 0, len(values))
	for _, value := range values {
		if value == mainProductSourceCategoryKeyOther {
			includeWithoutCategory = true
			continue
		}
		if value == mainProductSourceCategoryKeyEksmo {
			includeEksmo = true
			continue
		}
		if strings.HasPrefix(value, mainProductSourceCategoryKeyDomainPrefix) {
			encodedValue := strings.TrimPrefix(value, mainProductSourceCategoryKeyDomainPrefix)
			decoded, err := base64.RawURLEncoding.DecodeString(encodedValue)
			if err != nil {
				continue
			}
			path := cleanStringSlice(strings.Split(string(decoded), "\x1f"))
			if len(path) == 0 {
				continue
			}
			domain := strings.TrimSpace(strings.ToLower(path[0]))
			if domain == "" {
				continue
			}
			if _, exists := seenDomains[domain]; exists {
				continue
			}
			seenDomains[domain] = struct{}{}
			sourceDomainResult = append(sourceDomainResult, domain)
			continue
		}

		targetSeen := seenSource
		targetResult := &sourceResult
		encodedValue := value
		switch {
		case strings.HasPrefix(value, mainProductSourceCategoryKeyOtherPathPrefix):
			targetSeen = seenOther
			targetResult = &otherResult
			encodedValue = strings.TrimPrefix(value, mainProductSourceCategoryKeyOtherPathPrefix)
		case strings.HasPrefix(value, mainProductSourceCategoryKeyPathPrefix):
			encodedValue = strings.TrimPrefix(value, mainProductSourceCategoryKeyPathPrefix)
		}

		decoded, err := base64.RawURLEncoding.DecodeString(encodedValue)
		if err != nil {
			continue
		}
		path := cleanStringSlice(strings.Split(string(decoded), "\x1f"))
		if len(path) == 0 {
			continue
		}
		key := strings.Join(path, "\x1f")
		if _, exists := targetSeen[key]; exists {
			continue
		}
		targetSeen[key] = struct{}{}
		*targetResult = append(*targetResult, path)
	}
	if len(sourceResult) == 0 {
		sourceResult = nil
	}
	if len(otherResult) == 0 {
		otherResult = nil
	}
	if len(sourceDomainResult) == 0 {
		sourceDomainResult = nil
	}
	return sourceResult, otherResult, sourceDomainResult, includeWithoutCategory, includeEksmo
}

func isLikelyEksmoSourceCategoryPath(path []string) bool {
	normalized := cleanStringSlice(path)
	if len(normalized) == 0 {
		return false
	}

	first := strings.TrimSpace(strings.ToLower(normalized[0]))
	if first == "" || first == "без категории" {
		return false
	}
	// Host-like roots are usually external source domains.
	if looksLikeSourceHost(first) {
		return false
	}
	return true
}

func shouldHideMainSourceCategoryPath(path []string) bool {
	_ = path
	return false
}

func looksLikeSourceHost(value string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(value))
	if trimmed == "" {
		return false
	}
	if strings.ContainsAny(trimmed, " /\\\t\n\r") {
		return false
	}
	hostPattern := regexp.MustCompile(`^[a-z0-9-]+(\.[a-z0-9-]+)+$`)
	return hostPattern.MatchString(trimmed)
}

func encodeMainProductSourceCategoryPath(path []string, prefix string) string {
	if len(path) == 0 {
		return ""
	}
	return prefix + base64.RawURLEncoding.EncodeToString([]byte(strings.Join(path, "\x1f")))
}

func displayMainProductSourceCategorySegment(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		parsed, err := url.Parse(trimmed)
		if err == nil {
			host := strings.TrimSpace(strings.ToLower(parsed.Hostname()))
			if host != "" {
				return host
			}
		}
	}
	return trimmed
}

func buildMainProductSourceCategoryTree(paths [][]string, idPrefix string) []*MainProductSourceCategoryNode {
	type treeBuilderNode struct {
		node     *MainProductSourceCategoryNode
		children map[string]*treeBuilderNode
	}

	root := &treeBuilderNode{children: map[string]*treeBuilderNode{}}
	for _, rawPath := range paths {
		path := cleanStringSlice(rawPath)
		if len(path) == 0 {
			continue
		}
		current := root
		for depth, segment := range path {
			key := strings.TrimSpace(segment)
			if key == "" {
				continue
			}
			next, exists := current.children[key]
			if !exists {
				nodePath := append([]string{}, path[:depth+1]...)
				next = &treeBuilderNode{
					node: &MainProductSourceCategoryNode{
						ID:   encodeMainProductSourceCategoryPath(nodePath, idPrefix),
						Name: displayMainProductSourceCategorySegment(key),
						Path: nodePath,
					},
					children: map[string]*treeBuilderNode{},
				}
				current.children[key] = next
			}
			current = next
		}
	}

	var toList func(node *treeBuilderNode) []*MainProductSourceCategoryNode
	toList = func(node *treeBuilderNode) []*MainProductSourceCategoryNode {
		if len(node.children) == 0 {
			return nil
		}
		keys := make([]string, 0, len(node.children))
		for key := range node.children {
			keys = append(keys, key)
		}
		sort.Slice(keys, func(i, j int) bool {
			return strings.ToLower(keys[i]) < strings.ToLower(keys[j])
		})

		result := make([]*MainProductSourceCategoryNode, 0, len(keys))
		for _, key := range keys {
			child := node.children[key]
			if child.node == nil {
				continue
			}
			item := &MainProductSourceCategoryNode{
				ID:   child.node.ID,
				Name: child.node.Name,
				Path: append([]string{}, child.node.Path...),
			}
			item.Children = toList(child)
			result = append(result, item)
		}
		return result
	}

	return toList(root)
}

func (h *EksmoProductHandler) expandCategoryFilters(categoryIDs []primitive.ObjectID) []primitive.ObjectID {
	if len(categoryIDs) == 0 {
		return nil
	}
	if h.categoryLinker == nil {
		return categoryIDs
	}
	cacheCtx, cacheCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = h.categoryLinker.BuildCache(cacheCtx)
	cacheCancel()

	result := make([]primitive.ObjectID, 0, len(categoryIDs))
	seen := make(map[primitive.ObjectID]struct{}, len(categoryIDs))
	for _, categoryID := range categoryIDs {
		expanded := h.categoryLinker.GetCategoryAndDescendantIDs(categoryID)
		if len(expanded) == 0 {
			expanded = []primitive.ObjectID{categoryID}
		}
		for _, id := range expanded {
			if _, exists := seen[id]; exists {
				continue
			}
			seen[id] = struct{}{}
			result = append(result, id)
		}
	}

	return result
}

func cleanStringSlice(items []string) []string {
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

func uniqueStrings(items []string) []string {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	result := make([]string, 0, len(items))
	for _, item := range items {
		if _, exists := seen[item]; exists {
			continue
		}
		seen[item] = struct{}{}
		result = append(result, item)
	}
	return result
}

func splitProductIDsByFlags(products []models.EksmoProduct, flags []bool) ([]primitive.ObjectID, []primitive.ObjectID) {
	if len(products) == 0 || len(products) != len(flags) {
		return nil, nil
	}

	idsTrue := make([]primitive.ObjectID, 0, len(products))
	idsFalse := make([]primitive.ObjectID, 0, len(products))

	for i, product := range products {
		if product.ID.IsZero() {
			continue
		}
		if flags[i] {
			idsTrue = append(idsTrue, product.ID)
		} else {
			idsFalse = append(idsFalse, product.ID)
		}
	}

	return idsTrue, idsFalse
}

func mapObjectIDKeys(source map[primitive.ObjectID]struct{}) []primitive.ObjectID {
	if len(source) == 0 {
		return nil
	}
	result := make([]primitive.ObjectID, 0, len(source))
	for key := range source {
		result = append(result, key)
	}
	return result
}
