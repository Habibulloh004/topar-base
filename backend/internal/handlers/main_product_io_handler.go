package handlers

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"image"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"topar/backend/internal/models"
	"topar/backend/internal/repository"

	"github.com/gofiber/fiber/v2"
	"github.com/xuri/excelize/v2"
	"go.mongodb.org/mongo-driver/bson/primitive"

	_ "golang.org/x/image/bmp"
	_ "golang.org/x/image/webp"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
)

func requestIDFromCtx(c *fiber.Ctx) string {
	requestID := strings.TrimSpace(fmt.Sprint(c.Locals("requestid")))
	if requestID == "" || requestID == "<nil>" {
		requestID = strings.TrimSpace(c.Get(fiber.HeaderXRequestID))
	}
	if requestID == "" {
		requestID = "-"
	}
	return requestID
}

type mainProductColumn struct {
	Key    string
	Header string
}

type validatedMainProductImage struct {
	Ext                 string
	Format              string
	Width               int
	Height              int
	DetectedContentType string
	SHA256              string
}

var mainProductColumns = []mainProductColumn{
	{Key: "id", Header: "id"},
	{Key: "name", Header: "name"},
	{Key: "isbn", Header: "isbn"},
	{Key: "authorCover", Header: "author_cover"},
	{Key: "authorNames", Header: "author_names"},
	{Key: "tagNames", Header: "tag_names"},
	{Key: "genreNames", Header: "genre_names"},
	{Key: "isInfoComplete", Header: "is_info_complete"},
	{Key: "description", Header: "description"},
	{Key: "annotation", Header: "annotation"},
	{Key: "coverUrl", Header: "cover_url"},
	{Key: "coverUrls", Header: "cover_urls"},
	{Key: "ageRestriction", Header: "age_restriction"},
	{Key: "pages", Header: "pages"},
	{Key: "format", Header: "format"},
	{Key: "paperType", Header: "paper_type"},
	{Key: "bindingType", Header: "binding_type"},
	{Key: "characteristics", Header: "characteristics"},
	{Key: "boardGameType", Header: "board_game_type"},
	{Key: "productType", Header: "product_type"},
	{Key: "targetAudience", Header: "target_audience"},
	{Key: "minPlayers", Header: "min_players"},
	{Key: "maxPlayers", Header: "max_players"},
	{Key: "minGameDurationMinutes", Header: "min_game_duration_minutes"},
	{Key: "maxGameDurationMinutes", Header: "max_game_duration_minutes"},
	{Key: "material", Header: "material"},
	{Key: "subjectName", Header: "subject_name"},
	{Key: "nicheName", Header: "niche_name"},
	{Key: "brandName", Header: "brand_name"},
	{Key: "seriesName", Header: "series_name"},
	{Key: "publicationYear", Header: "publication_year"},
	{Key: "productWeight", Header: "product_weight"},
	{Key: "publisherName", Header: "publisher_name"},
	{Key: "quantity", Header: "quantity"},
	{Key: "price", Header: "price"},
	{Key: "categoryId", Header: "category_id"},
	{Key: "categoryPath", Header: "category_path"},
	{Key: "sourceGuidNom", Header: "source_guid_nom"},
	{Key: "sourceGuid", Header: "source_guid"},
	{Key: "sourceNomcode", Header: "source_nomcode"},
	{Key: "sourceProductId", Header: "source_product_id"},
}

type createMainProductRequest struct {
	Name                   string                         `json:"name"`
	ISBN                   string                         `json:"isbn"`
	AuthorCover            string                         `json:"authorCover"`
	AuthorNames            []string                       `json:"authorNames"`
	AuthorRefs             []models.EksmoProductAuthorRef `json:"authorRefs"`
	TagRefs                []models.EksmoProductTagRef    `json:"tagRefs"`
	GenreRefs              []models.EksmoProductGenreRef  `json:"genreRefs"`
	TagNames               []string                       `json:"tagNames"`
	GenreNames             []string                       `json:"genreNames"`
	IsInfoComplete         bool                           `json:"isInfoComplete"`
	Description            string                         `json:"description"`
	Annotation             string                         `json:"annotation"`
	CoverURL               string                         `json:"coverUrl"`
	CoverURLs              []string                       `json:"coverUrls"`
	Pages                  int                            `json:"pages"`
	Format                 string                         `json:"format"`
	PaperType              string                         `json:"paperType"`
	BindingType            string                         `json:"bindingType"`
	AgeRestriction         string                         `json:"ageRestriction"`
	Characteristics        string                         `json:"characteristics"`
	BoardGameType          string                         `json:"boardGameType"`
	ProductType            string                         `json:"productType"`
	TargetAudience         string                         `json:"targetAudience"`
	MinPlayers             int                            `json:"minPlayers"`
	MaxPlayers             int                            `json:"maxPlayers"`
	MinGameDurationMinutes int                            `json:"minGameDurationMinutes"`
	MaxGameDurationMinutes int                            `json:"maxGameDurationMinutes"`
	Material               string                         `json:"material"`
	SubjectName            string                         `json:"subjectName"`
	NicheName              string                         `json:"nicheName"`
	BrandName              string                         `json:"brandName"`
	SeriesName             string                         `json:"seriesName"`
	PublicationYear        int                            `json:"publicationYear"`
	ProductWeight          string                         `json:"productWeight"`
	PublisherName          string                         `json:"publisherName"`
	Quantity               float64                        `json:"quantity"`
	Price                  float64                        `json:"price"`
	CategoryID             string                         `json:"categoryId"`
	CategoryPath           []string                       `json:"categoryPath"`
	SourceGUIDNOM          string                         `json:"sourceGuidNom"`
	SourceGUID             string                         `json:"sourceGuid"`
	SourceNomCode          string                         `json:"sourceNomcode"`
}

func (h *EksmoProductHandler) CreateMainProduct(c *fiber.Ctx) error {
	requestID := requestIDFromCtx(c)
	log.Printf("[main_products_create] request_id=%s ip=%s method=%s path=%s", requestID, c.IP(), c.Method(), c.OriginalURL())

	if h.mainProductRepo == nil {
		log.Printf("[main_products_create] request_id=%s error=%q", requestID, "main product repository not configured")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	var req createMainProductRequest
	if err := c.BodyParser(&req); err != nil {
		log.Printf("[main_products_create] request_id=%s error=%q", requestID, "invalid request body")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		log.Printf("[main_products_create] request_id=%s error=%q", requestID, "name is required")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name is required"})
	}

	product, err := h.buildMainProductFromRequest(req)
	if err != nil {
		log.Printf("[main_products_create] request_id=%s error=%q", requestID, err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	product.Name = name

	log.Printf(
		"[main_products_create] request_id=%s name=%q category_id=%s cover_url=%q covers_count=%d",
		requestID,
		product.Name,
		product.CategoryID.Hex(),
		product.CoverURL,
		len(product.Covers),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	id, err := h.mainProductRepo.CreateManual(ctx, product)
	if err != nil {
		log.Printf("[main_products_create] request_id=%s error=%q", requestID, err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()
	log.Printf("[main_products_create] request_id=%s success=true id=%s", requestID, id.Hex())

	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "Main product created",
		"id":      id.Hex(),
	})
}

func (h *EksmoProductHandler) UploadMainProductImages(c *fiber.Ctx) error {
	requestID := requestIDFromCtx(c)
	log.Printf(
		"[main_products_upload] request_id=%s ip=%s method=%s path=%s content_type=%q content_length=%d",
		requestID,
		c.IP(),
		c.Method(),
		c.OriginalURL(),
		c.Get("Content-Type"),
		c.Request().Header.ContentLength(),
	)

	form, err := c.MultipartForm()
	if err != nil {
		log.Printf("[main_products_upload] request_id=%s error=%q", requestID, "multipart form with images is required")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "multipart form with images is required"})
	}

	files := form.File["images"]
	if len(files) == 0 {
		files = form.File["files"]
	}
	if len(files) == 0 {
		log.Printf("[main_products_upload] request_id=%s error=%q", requestID, "at least one image file is required")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "at least one image file is required (field: images)"})
	}
	if len(files) > 20 {
		log.Printf("[main_products_upload] request_id=%s error=%q file_count=%d", requestID, "too many files", len(files))
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "you can upload up to 20 images at once"})
	}

	uploadRoot := strings.TrimSpace(h.uploadsDir)
	if uploadRoot == "" {
		uploadRoot = "uploads"
	}
	uploadDir := filepath.Join(uploadRoot, "main-products")
	if err := os.MkdirAll(uploadDir, 0o755); err != nil {
		log.Printf(
			"[main_products_upload] request_id=%s upload_dir=%q error=%q cause=%q",
			requestID,
			uploadDir,
			"failed to prepare upload directory",
			err.Error(),
		)
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "failed to prepare upload directory"})
	}

	type uploadItem struct {
		Name        string `json:"name"`
		Size        int64  `json:"size"`
		SavedSize   int64  `json:"savedSize,omitempty"`
		Path        string `json:"path"`
		URL         string `json:"url"`
		Format      string `json:"format,omitempty"`
		Width       int    `json:"width,omitempty"`
		Height      int    `json:"height,omitempty"`
		ContentType string `json:"contentType,omitempty"`
		SHA256      string `json:"sha256,omitempty"`
	}
	items := make([]uploadItem, 0, len(files))

	for index, file := range files {
		log.Printf(
			"[main_products_upload] request_id=%s file_index=%d file_name=%q file_size=%d file_content_type=%q",
			requestID,
			index+1,
			file.Filename,
			file.Size,
			file.Header.Get("Content-Type"),
		)

		validated, err := validateMainProductImageFile(file)
		if err != nil {
			log.Printf("[main_products_upload] request_id=%s file_name=%q error=%q", requestID, file.Filename, err.Error())
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
		}

		fileName := fmt.Sprintf("%d_%d_%s%s", time.Now().UnixNano(), index+1, primitive.NewObjectID().Hex(), validated.Ext)
		diskPath := filepath.Join(uploadDir, fileName)
		if err := c.SaveFile(file, diskPath); err != nil {
			log.Printf("[main_products_upload] request_id=%s file_name=%q error=%q", requestID, file.Filename, err.Error())
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": fmt.Sprintf("failed to save file %q", file.Filename)})
		}
		savedSize := int64(0)
		if stat, statErr := os.Stat(diskPath); statErr == nil {
			savedSize = stat.Size()
		}

		relativePath := "/uploads/main-products/" + fileName
		log.Printf(
			"[main_products_upload] request_id=%s file_name=%q saved_path=%q format=%q width=%d height=%d detected_content_type=%q sha256=%s file_size=%d saved_size=%d",
			requestID,
			file.Filename,
			relativePath,
			validated.Format,
			validated.Width,
			validated.Height,
			validated.DetectedContentType,
			validated.SHA256,
			file.Size,
			savedSize,
		)
		items = append(items, uploadItem{
			Name:        file.Filename,
			Size:        file.Size,
			SavedSize:   savedSize,
			Path:        relativePath,
			URL:         strings.TrimRight(c.BaseURL(), "/") + relativePath,
			Format:      validated.Format,
			Width:       validated.Width,
			Height:      validated.Height,
			ContentType: validated.DetectedContentType,
			SHA256:      validated.SHA256,
		})
	}
	log.Printf("[main_products_upload] request_id=%s success=true uploaded_count=%d", requestID, len(items))

	return c.JSON(fiber.Map{
		"message":   "images uploaded",
		"requestId": requestID,
		"count":     len(items),
		"data":      items,
	})
}

const maxMainProductImageSizeBytes int64 = 20 * 1024 * 1024

func validateMainProductImageFile(file *multipart.FileHeader) (validatedMainProductImage, error) {
	result := validatedMainProductImage{}
	name := strings.TrimSpace(file.Filename)
	if name == "" {
		return result, fmt.Errorf("uploaded file name is empty")
	}
	if file.Size <= 0 {
		return result, fmt.Errorf("file %q is empty", name)
	}
	if file.Size > maxMainProductImageSizeBytes {
		return result, fmt.Errorf("file %q is too large (max 20MB)", name)
	}

	contentType := strings.ToLower(strings.TrimSpace(file.Header.Get("Content-Type")))
	if contentType != "" && !strings.HasPrefix(contentType, "image/") {
		return result, fmt.Errorf("file %q is not an image", name)
	}

	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(name)))
	if ext == ".jpg" {
		ext = ".jpeg"
	}
	if ext == ".svg" || strings.Contains(contentType, "svg") {
		if err := validateSVGImage(file); err != nil {
			return result, fmt.Errorf("file %q is not a valid SVG image", name)
		}
		svgHash, hashErr := calculateFileSHA256(file)
		if hashErr != nil {
			return result, fmt.Errorf("failed to checksum file %q", name)
		}
		result.Ext = ".svg"
		result.Format = "svg"
		result.DetectedContentType = "image/svg+xml"
		result.SHA256 = svgHash
		return result, nil
	}

	reader, err := file.Open()
	if err != nil {
		return result, fmt.Errorf("failed to read file %q", name)
	}
	defer reader.Close()

	header := make([]byte, 512)
	n, readErr := reader.Read(header)
	if readErr != nil && readErr != io.EOF {
		return result, fmt.Errorf("failed to inspect file %q", name)
	}
	if n == 0 {
		return result, fmt.Errorf("file %q is empty", name)
	}

	detectedContentType := strings.ToLower(http.DetectContentType(header[:n]))
	if !strings.HasPrefix(detectedContentType, "image/") {
		return result, fmt.Errorf("file %q is not an image", name)
	}

	reader2, err := file.Open()
	if err != nil {
		return result, fmt.Errorf("failed to validate image %q", name)
	}
	defer reader2.Close()

	config, format, err := image.DecodeConfig(reader2)
	if err != nil {
		return result, fmt.Errorf("file %q is not a valid image or is corrupted", name)
	}
	fileHash, hashErr := calculateFileSHA256(file)
	if hashErr != nil {
		return result, fmt.Errorf("failed to checksum file %q", name)
	}

	switch format {
	case "jpeg":
		result.Ext = ".jpg"
	case "png":
		result.Ext = ".png"
	case "gif":
		result.Ext = ".gif"
	case "bmp":
		result.Ext = ".bmp"
	case "webp":
		result.Ext = ".webp"
	default:
		return result, fmt.Errorf("unsupported image format for file %q", name)
	}

	result.Format = format
	result.Width = config.Width
	result.Height = config.Height
	result.DetectedContentType = detectedContentType
	result.SHA256 = fileHash
	return result, nil
}

func calculateFileSHA256(file *multipart.FileHeader) (string, error) {
	reader, err := file.Open()
	if err != nil {
		return "", err
	}
	defer reader.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, reader); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func validateSVGImage(file *multipart.FileHeader) error {
	reader, err := file.Open()
	if err != nil {
		return err
	}
	defer reader.Close()

	decoder := xml.NewDecoder(io.LimitReader(reader, 1024*1024))
	for {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		start, ok := token.(xml.StartElement)
		if !ok {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(start.Name.Local), "svg") {
			return nil
		}
		return fmt.Errorf("root element is not svg")
	}
}

func (h *EksmoProductHandler) UpdateMainProduct(c *fiber.Ctx) error {
	requestID := requestIDFromCtx(c)
	log.Printf("[main_products_update] request_id=%s ip=%s method=%s path=%s", requestID, c.IP(), c.Method(), c.OriginalURL())

	if h.mainProductRepo == nil {
		log.Printf("[main_products_update] request_id=%s error=%q", requestID, "main product repository not configured")
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	idStr := strings.TrimSpace(c.Params("id"))
	oid, err := primitive.ObjectIDFromHex(idStr)
	if err != nil {
		log.Printf("[main_products_update] request_id=%s product_id=%q error=%q", requestID, idStr, "invalid product id")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid product id"})
	}

	var req createMainProductRequest
	if err := c.BodyParser(&req); err != nil {
		log.Printf("[main_products_update] request_id=%s product_id=%s error=%q", requestID, idStr, "invalid request body")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid request body"})
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		log.Printf("[main_products_update] request_id=%s product_id=%s error=%q", requestID, idStr, "name is required")
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "name is required"})
	}

	product, err := h.buildMainProductFromRequest(req)
	if err != nil {
		log.Printf("[main_products_update] request_id=%s product_id=%s error=%q", requestID, idStr, err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	product.Name = name
	log.Printf(
		"[main_products_update] request_id=%s product_id=%s name=%q category_id=%s cover_url=%q covers_count=%d",
		requestID,
		idStr,
		product.Name,
		product.CategoryID.Hex(),
		product.CoverURL,
		len(product.Covers),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	updated, err := h.mainProductRepo.UpdateByID(ctx, oid, product)
	if err != nil {
		log.Printf("[main_products_update] request_id=%s product_id=%s error=%q", requestID, idStr, err.Error())
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if !updated {
		log.Printf("[main_products_update] request_id=%s product_id=%s error=%q", requestID, idStr, "product not found")
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "product not found"})
	}
	h.invalidateProductCaches()
	log.Printf("[main_products_update] request_id=%s product_id=%s success=true", requestID, idStr)

	return c.JSON(fiber.Map{
		"message": "Main product updated",
		"id":      idStr,
	})
}

func (h *EksmoProductHandler) buildMainProductFromRequest(req createMainProductRequest) (models.MainProduct, error) {
	categoryID := primitive.NilObjectID
	if rawCategoryID := strings.TrimSpace(req.CategoryID); rawCategoryID != "" {
		oid, err := primitive.ObjectIDFromHex(rawCategoryID)
		if err != nil {
			return models.MainProduct{}, fmt.Errorf("categoryId must be a valid ObjectID")
		}
		categoryID = oid
	}

	categoryPath := cleanStringSlice(req.CategoryPath)
	if !categoryID.IsZero() && len(categoryPath) == 0 && h.categoryLinker != nil {
		cacheCtx, cacheCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := h.categoryLinker.BuildCache(cacheCtx); err == nil {
			categoryPath = h.categoryLinker.GetCategoryPath(categoryID)
		}
		cacheCancel()
	}

	coverURLs := cleanStringSlice(req.CoverURLs)
	primaryCover := strings.TrimSpace(req.CoverURL)
	if primaryCover == "" && len(coverURLs) > 0 {
		primaryCover = coverURLs[0]
	}
	if primaryCover != "" {
		found := false
		for _, cover := range coverURLs {
			if cover == primaryCover {
				found = true
				break
			}
		}
		if !found {
			coverURLs = append([]string{primaryCover}, coverURLs...)
		}
	}
	covers := buildMainProductCoversMap(coverURLs)
	authorNames := cleanStringSlice(req.AuthorNames)
	authorRefs := cleanMainProductAuthorRefs(req.AuthorRefs)
	if len(authorRefs) == 0 && len(authorNames) > 0 {
		authorRefs = buildMainProductAuthorRefsFromNames(authorNames)
	}
	if len(authorNames) == 0 && len(authorRefs) > 0 {
		authorNames = extractMainProductAuthorNames(authorRefs)
	}

	tagNames := cleanStringSlice(req.TagNames)
	tagRefs := cleanMainProductTagRefs(req.TagRefs)
	if len(tagRefs) == 0 && len(tagNames) > 0 {
		tagRefs = buildMainProductTagRefsFromNames(tagNames)
	}
	if len(tagNames) == 0 && len(tagRefs) > 0 {
		tagNames = extractMainProductTagNames(tagRefs)
	}

	genreNames := cleanStringSlice(req.GenreNames)
	genreRefs := cleanMainProductGenreRefs(req.GenreRefs)
	if len(genreRefs) == 0 && len(genreNames) > 0 {
		genreRefs = buildMainProductGenreRefsFromNames(genreNames)
	}
	if len(genreNames) == 0 && len(genreRefs) > 0 {
		genreNames = extractMainProductGenreNames(genreRefs)
	}

	pages := req.Pages
	if pages < 0 {
		pages = 0
	}
	minPlayers := req.MinPlayers
	if minPlayers < 0 {
		minPlayers = 0
	}
	maxPlayers := req.MaxPlayers
	if maxPlayers < 0 {
		maxPlayers = 0
	}
	minGameDurationMinutes := req.MinGameDurationMinutes
	if minGameDurationMinutes < 0 {
		minGameDurationMinutes = 0
	}
	maxGameDurationMinutes := req.MaxGameDurationMinutes
	if maxGameDurationMinutes < 0 {
		maxGameDurationMinutes = 0
	}
	publicationYear := req.PublicationYear
	if publicationYear < 0 {
		publicationYear = 0
	}
	description := strings.TrimSpace(firstNonEmpty(req.Description, req.Annotation))
	annotation := strings.TrimSpace(firstNonEmpty(req.Annotation, req.Description))

	return models.MainProduct{
		Name:                   strings.TrimSpace(req.Name),
		ISBN:                   strings.TrimSpace(req.ISBN),
		AuthorCover:            strings.TrimSpace(req.AuthorCover),
		AuthorNames:            authorNames,
		AuthorRefs:             authorRefs,
		TagRefs:                tagRefs,
		GenreRefs:              genreRefs,
		TagNames:               tagNames,
		GenreNames:             genreNames,
		IsInfoComplete:         req.IsInfoComplete,
		Description:            description,
		Annotation:             annotation,
		CoverURL:               primaryCover,
		Covers:                 covers,
		Pages:                  pages,
		Format:                 strings.TrimSpace(req.Format),
		PaperType:              strings.TrimSpace(req.PaperType),
		BindingType:            strings.TrimSpace(req.BindingType),
		AgeRestriction:         strings.TrimSpace(req.AgeRestriction),
		Characteristics:        strings.TrimSpace(req.Characteristics),
		BoardGameType:          strings.TrimSpace(req.BoardGameType),
		ProductType:            strings.TrimSpace(req.ProductType),
		TargetAudience:         strings.TrimSpace(req.TargetAudience),
		MinPlayers:             minPlayers,
		MaxPlayers:             maxPlayers,
		MinGameDurationMinutes: minGameDurationMinutes,
		MaxGameDurationMinutes: maxGameDurationMinutes,
		Material:               strings.TrimSpace(req.Material),
		SubjectName:            strings.TrimSpace(req.SubjectName),
		NicheName:              strings.TrimSpace(req.NicheName),
		BrandName:              strings.TrimSpace(req.BrandName),
		SeriesName:             strings.TrimSpace(req.SeriesName),
		PublicationYear:        publicationYear,
		ProductWeight:          strings.TrimSpace(req.ProductWeight),
		PublisherName:          strings.TrimSpace(req.PublisherName),
		Quantity:               req.Quantity,
		Price:                  req.Price,
		CategoryID:             categoryID,
		CategoryPath:           categoryPath,
		SourceGUIDNOM:          strings.TrimSpace(req.SourceGUIDNOM),
		SourceGUID:             strings.TrimSpace(req.SourceGUID),
		SourceNomCode:          strings.TrimSpace(req.SourceNomCode),
	}, nil
}

func buildMainProductCoversMap(urls []string) map[string]string {
	if len(urls) == 0 {
		return nil
	}

	result := make(map[string]string, len(urls))
	for idx, raw := range urls {
		url := strings.TrimSpace(raw)
		if url == "" {
			continue
		}
		key := fmt.Sprintf("manual_%d", idx+1)
		result[key] = url
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func cleanMainProductAuthorRefs(values []models.EksmoProductAuthorRef) []models.EksmoProductAuthorRef {
	if len(values) == 0 {
		return nil
	}

	result := make([]models.EksmoProductAuthorRef, 0, len(values))
	for _, item := range values {
		item.GUID = strings.TrimSpace(item.GUID)
		item.Code = strings.TrimSpace(item.Code)
		item.Name = strings.TrimSpace(item.Name)
		if item.GUID == "" && item.Code == "" && item.Name == "" {
			continue
		}
		result = append(result, item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func cleanMainProductTagRefs(values []models.EksmoProductTagRef) []models.EksmoProductTagRef {
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

func cleanMainProductGenreRefs(values []models.EksmoProductGenreRef) []models.EksmoProductGenreRef {
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

func buildMainProductAuthorRefsFromNames(names []string) []models.EksmoProductAuthorRef {
	if len(names) == 0 {
		return nil
	}
	result := make([]models.EksmoProductAuthorRef, 0, len(names))
	for _, name := range cleanStringSlice(names) {
		result = append(result, models.EksmoProductAuthorRef{Name: name})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func buildMainProductTagRefsFromNames(names []string) []models.EksmoProductTagRef {
	if len(names) == 0 {
		return nil
	}
	result := make([]models.EksmoProductTagRef, 0, len(names))
	for _, name := range cleanStringSlice(names) {
		result = append(result, models.EksmoProductTagRef{Name: name})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func buildMainProductGenreRefsFromNames(names []string) []models.EksmoProductGenreRef {
	if len(names) == 0 {
		return nil
	}
	result := make([]models.EksmoProductGenreRef, 0, len(names))
	for _, name := range cleanStringSlice(names) {
		result = append(result, models.EksmoProductGenreRef{Name: name})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func extractMainProductAuthorNames(values []models.EksmoProductAuthorRef) []string {
	if len(values) == 0 {
		return nil
	}
	names := make([]string, 0, len(values))
	for _, item := range values {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return cleanStringSlice(names)
}

func extractMainProductTagNames(values []models.EksmoProductTagRef) []string {
	if len(values) == 0 {
		return nil
	}
	names := make([]string, 0, len(values))
	for _, item := range values {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return cleanStringSlice(names)
}

func extractMainProductGenreNames(values []models.EksmoProductGenreRef) []string {
	if len(values) == 0 {
		return nil
	}
	names := make([]string, 0, len(values))
	for _, item := range values {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return cleanStringSlice(names)
}

func (h *EksmoProductHandler) ExportMainProducts(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	exportFormat := strings.ToLower(strings.TrimSpace(c.Query("format", "csv")))
	if exportFormat != "csv" && exportFormat != "xlsx" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "format must be csv or xlsx"})
	}

	params := repository.MainProductFilterParams{
		Search:          strings.TrimSpace(c.Query("search")),
		WithoutCategory: parseBoolQuery(c, "withoutCategory", false),
		WithoutISBN:     parseBoolQuery(c, "withoutIsbn", false),
		BillzSyncable:   parseBillzSyncFilterMode(strings.TrimSpace(c.Query("billzSync"))),
		InfoComplete:    parseInfoCompleteFilterMode(strings.TrimSpace(c.Query("infoComplete"))),
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

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	products, err := h.mainProductRepo.ListAllWithFilters(ctx, params)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	var content []byte
	var contentType string
	var fileExt string
	if exportFormat == "xlsx" {
		content, err = buildMainProductsXLSX(products)
		contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
		fileExt = "xlsx"
	} else {
		content, err = buildMainProductsCSV(products)
		contentType = "text/csv; charset=utf-8"
		fileExt = "csv"
	}
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}

	filename := fmt.Sprintf("main_products_%s.%s", time.Now().UTC().Format("20060102_150405"), fileExt)
	c.Set("Content-Type", contentType)
	c.Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", filename))
	return c.Send(content)
}

func (h *EksmoProductHandler) ImportMainProducts(c *fiber.Ctx) error {
	if h.mainProductRepo == nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "main product repository not configured"})
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "file is required"})
	}

	importFormat := detectMainProductImportFormat(fileHeader.Filename, c.FormValue("format"))
	if importFormat == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "unsupported file format: use csv or xlsx"})
	}

	file, err := fileHeader.Open()
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "failed to open uploaded file"})
	}
	defer file.Close()

	raw, err := io.ReadAll(file)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "failed to read uploaded file"})
	}
	if len(raw) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "uploaded file is empty"})
	}

	var parsed []models.MainProduct
	switch importFormat {
	case "xlsx":
		parsed, err = parseMainProductsXLSX(raw)
	default:
		parsed, err = parseMainProductsCSV(raw)
	}
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": err.Error()})
	}
	if len(parsed) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "no valid rows found in file"})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	inserted, modified, skipped, err := h.mainProductRepo.UpsertImported(ctx, parsed)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	h.invalidateProductCaches()

	return c.JSON(fiber.Map{
		"message":   "Import completed",
		"processed": len(parsed),
		"inserted":  inserted,
		"modified":  modified,
		"skipped":   skipped,
	})
}

func buildMainProductsCSV(products []models.MainProduct) ([]byte, error) {
	var buffer bytes.Buffer
	writer := csv.NewWriter(&buffer)

	headers := make([]string, 0, len(mainProductColumns))
	for _, col := range mainProductColumns {
		headers = append(headers, col.Header)
	}
	if err := writer.Write(headers); err != nil {
		return nil, err
	}

	for _, product := range products {
		if err := writer.Write(mainProductCSVRow(product)); err != nil {
			return nil, err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func buildMainProductsXLSX(products []models.MainProduct) ([]byte, error) {
	book := excelize.NewFile()
	const sheetName = "main_products"

	defaultSheet := book.GetSheetName(0)
	if defaultSheet == "" {
		defaultSheet = "Sheet1"
	}
	if defaultSheet != sheetName {
		book.SetSheetName(defaultSheet, sheetName)
	}

	headers := make([]string, 0, len(mainProductColumns))
	for _, col := range mainProductColumns {
		headers = append(headers, col.Header)
	}

	for colIdx, header := range headers {
		cell, _ := excelize.CoordinatesToCellName(colIdx+1, 1)
		_ = book.SetCellValue(sheetName, cell, header)
	}

	for rowIdx, product := range products {
		row := mainProductCSVRow(product)
		for colIdx, value := range row {
			cell, _ := excelize.CoordinatesToCellName(colIdx+1, rowIdx+2)
			_ = book.SetCellValue(sheetName, cell, value)
		}
	}

	buffer, err := book.WriteToBuffer()
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func detectMainProductImportFormat(filename, requested string) string {
	format := strings.ToLower(strings.TrimSpace(requested))
	if format == "csv" || format == "xlsx" {
		return format
	}

	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(filename)))
	switch ext {
	case ".csv":
		return "csv"
	case ".xlsx":
		return "xlsx"
	default:
		return ""
	}
}

func parseMainProductsCSV(raw []byte) ([]models.MainProduct, error) {
	comma := detectCSVDelimiter(raw)
	reader := csv.NewReader(bytes.NewReader(raw))
	reader.Comma = comma
	reader.FieldsPerRecord = -1
	reader.TrimLeadingSpace = true

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("file must include header and at least one data row")
	}

	return parseMainProductRows(rows)
}

func parseMainProductsXLSX(raw []byte) ([]models.MainProduct, error) {
	book, err := excelize.OpenReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}

	sheets := book.GetSheetList()
	if len(sheets) == 0 {
		return nil, fmt.Errorf("xlsx file has no sheets")
	}

	rows, err := book.GetRows(sheets[0])
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("xlsx must include header and at least one data row")
	}

	return parseMainProductRows(rows)
}

func parseMainProductRows(rows [][]string) ([]models.MainProduct, error) {
	headerIndex := buildMainProductHeaderIndex(rows[0])
	products := make([]models.MainProduct, 0, len(rows)-1)

	for i := 1; i < len(rows); i++ {
		row := rows[i]
		if isMainProductRowEmpty(row) {
			continue
		}
		product := models.MainProduct{}

		if value := readMainProductCell(row, headerIndex, "id"); value != "" {
			if oid, err := primitive.ObjectIDFromHex(value); err == nil {
				product.ID = oid
			}
		}
		product.Name = readMainProductCell(row, headerIndex, "name")
		product.ISBN = readMainProductCell(row, headerIndex, "isbn")
		product.AuthorCover = firstNonEmpty(
			readMainProductCell(row, headerIndex, "authorCover"),
			readMainProductCell(row, headerIndex, "author"),
		)
		product.AuthorNames = splitMainProductList(firstNonEmpty(
			readMainProductCell(row, headerIndex, "authorNames"),
			readMainProductCell(row, headerIndex, "author"),
		))
		product.TagNames = splitMainProductList(readMainProductCell(row, headerIndex, "tagNames"))
		product.GenreNames = splitMainProductList(readMainProductCell(row, headerIndex, "genreNames"))
		product.IsInfoComplete = parseMainProductBool(readMainProductCell(row, headerIndex, "isInfoComplete"))
		product.Description = firstNonEmpty(
			readMainProductCell(row, headerIndex, "description"),
			readMainProductCell(row, headerIndex, "annotation"),
			readMainProductCell(row, headerIndex, "shortDescription"),
		)
		product.Annotation = firstNonEmpty(
			readMainProductCell(row, headerIndex, "annotation"),
			readMainProductCell(row, headerIndex, "shortDescription"),
			product.Description,
		)
		coverURLs := splitMainProductLinkList(readMainProductCell(row, headerIndex, "coverUrls"))
		product.CoverURL = extractPrimaryLink(readMainProductCell(row, headerIndex, "coverUrl"))
		if product.CoverURL == "" && len(coverURLs) > 0 {
			product.CoverURL = coverURLs[0]
		}
		product.Covers = buildMainProductCoversMap(coverURLs)
		product.AgeRestriction = readMainProductCell(row, headerIndex, "ageRestriction")
		product.Pages = parseMainProductInt(readMainProductCell(row, headerIndex, "pages"))
		product.Format = readMainProductCell(row, headerIndex, "format")
		product.PaperType = readMainProductCell(row, headerIndex, "paperType")
		product.BindingType = readMainProductCell(row, headerIndex, "bindingType")
		product.Characteristics = readMainProductCell(row, headerIndex, "characteristics")
		product.BoardGameType = readMainProductCell(row, headerIndex, "boardGameType")
		product.ProductType = readMainProductCell(row, headerIndex, "productType")
		product.TargetAudience = readMainProductCell(row, headerIndex, "targetAudience")
		product.MinPlayers = parseMainProductInt(readMainProductCell(row, headerIndex, "minPlayers"))
		product.MaxPlayers = parseMainProductInt(readMainProductCell(row, headerIndex, "maxPlayers"))
		product.MinGameDurationMinutes = parseMainProductInt(readMainProductCell(row, headerIndex, "minGameDurationMinutes"))
		product.MaxGameDurationMinutes = parseMainProductInt(readMainProductCell(row, headerIndex, "maxGameDurationMinutes"))
		product.Material = readMainProductCell(row, headerIndex, "material")
		product.SubjectName = firstNonEmpty(
			readMainProductCell(row, headerIndex, "subjectName"),
			readMainProductCell(row, headerIndex, "category"),
		)
		product.NicheName = firstNonEmpty(
			readMainProductCell(row, headerIndex, "nicheName"),
			readMainProductCell(row, headerIndex, "subcategory"),
		)
		product.BrandName = readMainProductCell(row, headerIndex, "brandName")
		product.SeriesName = readMainProductCell(row, headerIndex, "seriesName")
		product.PublicationYear = parseMainProductInt(readMainProductCell(row, headerIndex, "publicationYear"))
		product.ProductWeight = readMainProductCell(row, headerIndex, "productWeight")
		product.PublisherName = readMainProductCell(row, headerIndex, "publisherName")
		product.SourceGUIDNOM = readMainProductCell(row, headerIndex, "sourceGuidNom")
		product.SourceGUID = readMainProductCell(row, headerIndex, "sourceGuid")
		product.SourceNomCode = readMainProductCell(row, headerIndex, "sourceNomcode")

		if value := readMainProductCell(row, headerIndex, "sourceProductId"); value != "" {
			if oid, err := primitive.ObjectIDFromHex(value); err == nil {
				product.SourceProductID = oid
			}
		}
		if value := readMainProductCell(row, headerIndex, "categoryId"); value != "" {
			if oid, err := primitive.ObjectIDFromHex(value); err == nil {
				product.CategoryID = oid
			}
		}

		product.CategoryPath = splitMainProductPath(firstNonEmpty(
			readMainProductCell(row, headerIndex, "categoryPath"),
			buildCategoryPathFromColumns(
				readMainProductCell(row, headerIndex, "category"),
				readMainProductCell(row, headerIndex, "subcategory"),
			),
		))
		product.Quantity = parseMainProductFloat(readMainProductCell(row, headerIndex, "quantity"))
		product.Price = parseMainProductFloat(readMainProductCell(row, headerIndex, "price"))

		if strings.TrimSpace(product.Name) == "" {
			continue
		}
		products = append(products, product)
	}

	if len(products) == 0 {
		return nil, fmt.Errorf("no valid rows with non-empty name found")
	}
	return products, nil
}

func buildMainProductHeaderIndex(header []string) map[string]int {
	index := make(map[string]int, len(header))
	for i, raw := range header {
		switch normalizeMainProductHeader(raw) {
		case "id", "_id":
			index["id"] = i
		case "name", "title", "productname":
			index["name"] = i
		case "isbn":
			index["isbn"] = i
		case "authorcover", "авторнаобложке", "авторы":
			index["authorCover"] = i
		case "author":
			index["authorCover"] = i
			index["author"] = i
		case "authornames", "authors":
			index["authorNames"] = i
		case "tagnames", "tags", "теги":
			index["tagNames"] = i
		case "genrenames", "genres", "жанры":
			index["genreNames"] = i
		case "isinfocomplete", "infocomplete", "fullinfo", "isfullinfo":
			index["isInfoComplete"] = i
		case "полнаяинформация", "инфополная", "полнотаинформации":
			index["isInfoComplete"] = i
		case "автор":
			index["author"] = i
		case "description", "описание":
			index["description"] = i
		case "annotation":
			index["annotation"] = i
		case "краткоеописание", "короткоеописание", "shortdescription":
			index["shortDescription"] = i
		case "coverurl", "image", "imageurl":
			index["coverUrl"] = i
		case "coverurls", "covers", "imagelinks", "coverlinks":
			index["coverUrls"] = i
		case "ссылкинаобложки", "ссылкинаизображения", "всеобложки", "всеизображения":
			index["coverUrls"] = i
		case "фотоилиссылкинафото", "фото", "ссылканафото", "ссылкинафото", "изображение", "photophotolinks", "photo", "photolinks":
			index["coverUrl"] = i
		case "agerestriction", "age":
			index["ageRestriction"] = i
		case "возрастноеограничение", "возврастноеограничение":
			index["ageRestriction"] = i
		case "pages", "pagecount":
			index["pages"] = i
		case "страницы", "количествостраниц":
			index["pages"] = i
		case "format":
			index["format"] = i
		case "формат":
			index["format"] = i
		case "papertype", "paper":
			index["paperType"] = i
		case "типбумаги", "бумага":
			index["paperType"] = i
		case "bindingtype", "binding":
			index["bindingType"] = i
		case "типпереплета", "переплет":
			index["bindingType"] = i
		case "characteristics":
			index["characteristics"] = i
		case "характеристики", "характеристика":
			index["characteristics"] = i
		case "boardgametype":
			index["boardGameType"] = i
		case "виднастольнойигры":
			index["boardGameType"] = i
		case "producttype", "type":
			index["productType"] = i
		case "тип":
			index["productType"] = i
		case "targetaudience":
			index["targetAudience"] = i
		case "целеваяаудитория":
			index["targetAudience"] = i
		case "minplayers":
			index["minPlayers"] = i
		case "минимальноечислоигроков":
			index["minPlayers"] = i
		case "maxplayers":
			index["maxPlayers"] = i
		case "максимальноечислоигроков":
			index["maxPlayers"] = i
		case "mingamedurationminutes":
			index["minGameDurationMinutes"] = i
		case "минимальнаяпродолжительностьпартиимин", "минимальнаядлительностьпартиимин":
			index["minGameDurationMinutes"] = i
		case "maxgamedurationminutes":
			index["maxGameDurationMinutes"] = i
		case "максимальнаяпродолжительностьпартиимин", "максимальнаядлительностьпартиимин":
			index["maxGameDurationMinutes"] = i
		case "material":
			index["material"] = i
		case "материал":
			index["material"] = i
		case "subjectname", "subject":
			index["subjectName"] = i
		case "категория":
			index["category"] = i
		case "nichename", "niche":
			index["nicheName"] = i
		case "подкатегория", "subcategory":
			index["subcategory"] = i
		case "brandname", "brand":
			index["brandName"] = i
		case "бренд":
			index["brandName"] = i
		case "seriesname", "serie", "series":
			index["seriesName"] = i
		case "серия":
			index["seriesName"] = i
		case "publicationyear", "yearofpublication":
			index["publicationYear"] = i
		case "годиздания":
			index["publicationYear"] = i
		case "productweight", "weight":
			index["productWeight"] = i
		case "вестовара", "вес":
			index["productWeight"] = i
		case "publishername", "publisher":
			index["publisherName"] = i
		case "издатель", "издательство":
			index["publisherName"] = i
		case "quantity", "qty":
			index["quantity"] = i
		case "количество", "остаток":
			index["quantity"] = i
		case "price", "cost":
			index["price"] = i
		case "цена", "стоимость":
			index["price"] = i
		case "categoryid":
			index["categoryId"] = i
		case "categorypath":
			index["categoryPath"] = i
		case "путькатегории":
			index["categoryPath"] = i
		case "sourceguidnom", "guidnom", "guid_nom":
			index["sourceGuidNom"] = i
		case "sourceguid", "guid":
			index["sourceGuid"] = i
		case "sourcenomcode", "nomcode":
			index["sourceNomcode"] = i
		case "икпу", "ikpu":
			index["sourceNomcode"] = i
		case "sourceproductid":
			index["sourceProductId"] = i
		case "названиетовара", "наименованиетовара", "товар", "название":
			index["name"] = i
		case "isbn13":
			index["isbn"] = i
		}
	}
	return index
}

func normalizeMainProductHeader(value string) string {
	lower := strings.ToLower(stripUTF8BOM(strings.TrimSpace(value)))
	lower = strings.ReplaceAll(lower, " ", "")
	lower = strings.ReplaceAll(lower, "-", "")
	lower = strings.ReplaceAll(lower, ".", "")
	lower = strings.ReplaceAll(lower, "/", "")
	lower = strings.ReplaceAll(lower, "\\", "")
	lower = strings.ReplaceAll(lower, ":", "")
	lower = strings.ReplaceAll(lower, ";", "")
	lower = strings.ReplaceAll(lower, "(", "")
	lower = strings.ReplaceAll(lower, ")", "")
	return strings.ReplaceAll(lower, "_", "")
}

func readMainProductCell(row []string, headerIndex map[string]int, key string) string {
	idx, exists := headerIndex[key]
	if !exists || idx < 0 || idx >= len(row) {
		return ""
	}
	return stripUTF8BOM(strings.TrimSpace(row[idx]))
}

func isMainProductRowEmpty(row []string) bool {
	for _, cell := range row {
		if strings.TrimSpace(cell) != "" {
			return false
		}
	}
	return true
}

func parseMainProductFloat(value string) float64 {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return 0
	}
	normalized = stripUTF8BOM(normalized)
	normalized = strings.ReplaceAll(normalized, " ", "")
	normalized = strings.ReplaceAll(normalized, "\u00a0", "")
	if normalized == "" {
		return 0
	}

	// If only comma exists, treat it as thousands separator when right part has 3 digits.
	if strings.Contains(normalized, ",") && !strings.Contains(normalized, ".") {
		parts := strings.Split(normalized, ",")
		if len(parts) > 2 {
			normalized = strings.Join(parts, "")
		} else if len(parts) == 2 {
			left := parts[0]
			right := parts[1]
			if len(right) == 3 && len(left) > 0 {
				normalized = left + right
			} else {
				normalized = left + "." + right
			}
		}
	}

	// If both separators exist, choose the rightmost as decimal separator.
	if strings.Contains(normalized, ",") && strings.Contains(normalized, ".") {
		lastComma := strings.LastIndex(normalized, ",")
		lastDot := strings.LastIndex(normalized, ".")
		if lastComma > lastDot {
			normalized = strings.ReplaceAll(normalized, ".", "")
			normalized = strings.ReplaceAll(normalized, ",", ".")
		} else {
			normalized = strings.ReplaceAll(normalized, ",", "")
		}
	}

	parsed, err := strconv.ParseFloat(normalized, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func parseMainProductInt(value string) int {
	parsed := parseMainProductFloat(value)
	if parsed <= 0 {
		return 0
	}
	return int(parsed)
}

func parseMainProductBool(value string) bool {
	normalized := strings.ToLower(strings.TrimSpace(stripUTF8BOM(value)))
	switch normalized {
	case "1", "true", "yes", "y", "on", "да", "истина":
		return true
	default:
		return false
	}
}

func splitMainProductList(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	value = strings.ReplaceAll(value, "|", ",")
	value = strings.ReplaceAll(value, ";", ",")
	parts := strings.Split(value, ",")
	return cleanStringSlice(parts)
}

func splitMainProductPath(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if strings.Contains(value, " / ") {
		return cleanStringSlice(strings.Split(value, " / "))
	}
	value = strings.ReplaceAll(value, "|", ",")
	return cleanStringSlice(strings.Split(value, ","))
}

func splitMainProductLinkList(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		switch r {
		case '\n', '\r', '|', ';', ',':
			return true
		default:
			return false
		}
	})
	return cleanStringSlice(parts)
}

func buildMainProductCoverURLs(primary string, covers map[string]string) []string {
	result := []string{}
	seen := map[string]struct{}{}
	push := func(value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if _, exists := seen[value]; exists {
			return
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}

	push(primary)
	if len(covers) == 0 {
		return result
	}

	keys := make([]string, 0, len(covers))
	for key := range covers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		push(covers[key])
	}
	return result
}

func mainProductCSVRow(product models.MainProduct) []string {
	row := make([]string, 0, len(mainProductColumns))
	for _, col := range mainProductColumns {
		switch col.Key {
		case "id":
			if !product.ID.IsZero() {
				row = append(row, product.ID.Hex())
			} else {
				row = append(row, "")
			}
		case "name":
			row = append(row, product.Name)
		case "isbn":
			row = append(row, product.ISBN)
		case "authorCover":
			row = append(row, product.AuthorCover)
		case "authorNames":
			row = append(row, strings.Join(product.AuthorNames, " | "))
		case "tagNames":
			row = append(row, strings.Join(product.TagNames, " | "))
		case "genreNames":
			row = append(row, strings.Join(product.GenreNames, " | "))
		case "isInfoComplete":
			if product.IsInfoComplete {
				row = append(row, "1")
			} else {
				row = append(row, "0")
			}
		case "description":
			row = append(row, product.Description)
		case "annotation":
			row = append(row, product.Annotation)
		case "coverUrl":
			row = append(row, product.CoverURL)
		case "coverUrls":
			row = append(row, strings.Join(buildMainProductCoverURLs(product.CoverURL, product.Covers), " | "))
		case "ageRestriction":
			row = append(row, product.AgeRestriction)
		case "pages":
			if product.Pages > 0 {
				row = append(row, strconv.Itoa(product.Pages))
			} else {
				row = append(row, "")
			}
		case "format":
			row = append(row, product.Format)
		case "paperType":
			row = append(row, product.PaperType)
		case "bindingType":
			row = append(row, product.BindingType)
		case "characteristics":
			row = append(row, product.Characteristics)
		case "boardGameType":
			row = append(row, product.BoardGameType)
		case "productType":
			row = append(row, product.ProductType)
		case "targetAudience":
			row = append(row, product.TargetAudience)
		case "minPlayers":
			if product.MinPlayers > 0 {
				row = append(row, strconv.Itoa(product.MinPlayers))
			} else {
				row = append(row, "")
			}
		case "maxPlayers":
			if product.MaxPlayers > 0 {
				row = append(row, strconv.Itoa(product.MaxPlayers))
			} else {
				row = append(row, "")
			}
		case "minGameDurationMinutes":
			if product.MinGameDurationMinutes > 0 {
				row = append(row, strconv.Itoa(product.MinGameDurationMinutes))
			} else {
				row = append(row, "")
			}
		case "maxGameDurationMinutes":
			if product.MaxGameDurationMinutes > 0 {
				row = append(row, strconv.Itoa(product.MaxGameDurationMinutes))
			} else {
				row = append(row, "")
			}
		case "material":
			row = append(row, product.Material)
		case "subjectName":
			row = append(row, product.SubjectName)
		case "nicheName":
			row = append(row, product.NicheName)
		case "brandName":
			row = append(row, product.BrandName)
		case "seriesName":
			row = append(row, product.SeriesName)
		case "publicationYear":
			if product.PublicationYear > 0 {
				row = append(row, strconv.Itoa(product.PublicationYear))
			} else {
				row = append(row, "")
			}
		case "productWeight":
			row = append(row, product.ProductWeight)
		case "publisherName":
			row = append(row, product.PublisherName)
		case "quantity":
			row = append(row, formatMainProductFloat(product.Quantity))
		case "price":
			row = append(row, formatMainProductFloat(product.Price))
		case "categoryId":
			if !product.CategoryID.IsZero() {
				row = append(row, product.CategoryID.Hex())
			} else {
				row = append(row, "")
			}
		case "categoryPath":
			row = append(row, strings.Join(product.CategoryPath, " / "))
		case "sourceGuidNom":
			row = append(row, product.SourceGUIDNOM)
		case "sourceGuid":
			row = append(row, product.SourceGUID)
		case "sourceNomcode":
			row = append(row, product.SourceNomCode)
		case "sourceProductId":
			if !product.SourceProductID.IsZero() {
				row = append(row, product.SourceProductID.Hex())
			} else {
				row = append(row, "")
			}
		}
	}
	return row
}

func formatMainProductFloat(value float64) string {
	if value == 0 {
		return ""
	}
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func stripUTF8BOM(value string) string {
	return strings.TrimPrefix(value, "\ufeff")
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

func extractPrimaryLink(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	separators := []string{";", ",", "|", " "}
	for _, sep := range separators {
		if !strings.Contains(trimmed, sep) {
			continue
		}
		parts := strings.Split(trimmed, sep)
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if strings.HasPrefix(strings.ToLower(part), "http://") || strings.HasPrefix(strings.ToLower(part), "https://") {
				return part
			}
		}
	}
	return trimmed
}

func buildCategoryPathFromColumns(category, subcategory string) string {
	category = strings.TrimSpace(category)
	subcategory = strings.TrimSpace(subcategory)
	switch {
	case category != "" && subcategory != "":
		return category + " / " + subcategory
	case category != "":
		return category
	case subcategory != "":
		return subcategory
	default:
		return ""
	}
}

func detectCSVDelimiter(raw []byte) rune {
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	if !scanner.Scan() {
		return ','
	}
	line := scanner.Text()
	line = stripUTF8BOM(line)
	commaCount := strings.Count(line, ",")
	semicolonCount := strings.Count(line, ";")
	tabCount := strings.Count(line, "\t")
	if semicolonCount > commaCount && semicolonCount >= tabCount {
		return ';'
	}
	if tabCount > commaCount && tabCount > semicolonCount {
		return '\t'
	}
	return ','
}
