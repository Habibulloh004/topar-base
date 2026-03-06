package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"topar/backend/internal/config"
	"topar/backend/internal/repository"
)

type BillzSyncService struct {
	cfg        config.Config
	repo       *repository.MainProductRepository
	httpClient *http.Client

	mu      sync.Mutex
	running bool
}

type BillzSyncResult struct {
	FetchedProducts int
	UniqueBarcodes  int
	Candidates      int
	Matched         int
	Updated         int
}

var ErrBillzSyncRunning = errors.New("billz sync is already running")

type billzLoginResponse struct {
	AccessToken string `json:"access_token"`
	Data        struct {
		AccessToken string `json:"access_token"`
	} `json:"data"`
}

type billzProductsResponse struct {
	Count    int            `json:"count"`
	Products []billzProduct `json:"products"`
}

type billzProduct struct {
	ID                    string             `json:"id"`
	Barcode               string             `json:"barcode"`
	ShopMeasurementValues []billzMeasurement `json:"shop_measurement_values"`
	ShopPrices            []billzShopPrice   `json:"shop_prices"`
}

type billzMeasurement struct {
	ShopName                    string `json:"shop_name"`
	ActiveMeasurementValue      any    `json:"active_measurement_value"`
	ActiveMeasurementValueCamel any    `json:"activeMeasurementValue"`
}

type billzShopPrice struct {
	ShopName         string `json:"shop_name"`
	RetailPrice      any    `json:"retail_price"`
	RetailPriceCamel any    `json:"retailPrice"`
}

type billzStockPrice struct {
	Quantity float64
	Price    float64
}

const (
	billzProductsBasePageLimit  = 1000
	billzProductsSegmentLimit   = 500
	billzProductsMaxWindow      = 10000
	billzSegmentMaxDepthDigits  = 3
	billzSegmentProgressLogStep = 1000
	billzRequestMaxRetries      = 5
	billzRetryBaseDelay         = 500 * time.Millisecond
	billzRetryMaxDelay          = 8 * time.Second
	billzMinRequestTimeout      = 120 * time.Second
)

func NewBillzSyncService(cfg config.Config, repo *repository.MainProductRepository) *BillzSyncService {
	timeout := time.Duration(cfg.BillzTimeoutS) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	if timeout < billzMinRequestTimeout {
		timeout = billzMinRequestTimeout
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   20 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       0,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &BillzSyncService{
		cfg:  cfg,
		repo: repo,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
	}
}

func (s *BillzSyncService) Start(ctx context.Context) {
	if s.repo == nil {
		log.Printf("billz sync disabled: main product repository is not configured")
		return
	}
	if !s.cfg.BillzSyncEnabled {
		log.Printf("billz sync disabled: BILLZ_SYNC_ENABLED=false")
		return
	}
	if strings.TrimSpace(s.cfg.BillzAPISecret) == "" {
		log.Printf("billz sync disabled: BILLZ_API_SECRET_KEY is empty")
		return
	}

	interval := s.cfg.BillzSyncEvery
	if interval <= 0 {
		interval = time.Hour
	}

	log.Printf("billz sync scheduler started (interval=%s)", interval)

	go func() {
		s.runCycle(ctx)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Printf("billz sync scheduler stopped")
				return
			case <-ticker.C:
				s.runCycle(ctx)
			}
		}
	}()
}

func (s *BillzSyncService) runCycle(parentCtx context.Context) {
	if !s.startRun() {
		log.Printf("billz sync is already running, skipping this tick")
		return
	}
	defer s.finishRun()

	cycleTimeout := time.Duration(s.cfg.BillzTimeoutS)*time.Second + 20*time.Minute
	if cycleTimeout < 20*time.Minute {
		cycleTimeout = 20 * time.Minute
	}
	ctx, cancel := context.WithTimeout(parentCtx, cycleTimeout)
	defer cancel()

	started := time.Now()
	result, err := s.SyncOnce(ctx)
	duration := time.Since(started).Round(time.Millisecond)
	if err != nil {
		log.Printf("billz sync failed after %s: %v", duration, err)
		return
	}

	log.Printf(
		"billz sync completed in %s: fetched=%d, uniqueBarcodes=%d, candidates=%d, matched=%d, updated=%d",
		duration,
		result.FetchedProducts,
		result.UniqueBarcodes,
		result.Candidates,
		result.Matched,
		result.Updated,
	)
}

func (s *BillzSyncService) SyncNow(ctx context.Context) (BillzSyncResult, error) {
	result := BillzSyncResult{}
	if !s.startRun() {
		return result, ErrBillzSyncRunning
	}
	defer s.finishRun()

	return s.SyncOnce(ctx)
}

func (s *BillzSyncService) SyncOnce(ctx context.Context) (BillzSyncResult, error) {
	result := BillzSyncResult{}

	token, err := s.login(ctx)
	if err != nil {
		return result, err
	}

	products, err := s.fetchProducts(ctx, token)
	if err != nil {
		return result, err
	}
	result.FetchedProducts = len(products)

	billzByBarcode := s.buildBarcodeMap(products)
	result.UniqueBarcodes = len(billzByBarcode)
	if len(billzByBarcode) == 0 {
		return result, nil
	}

	candidates, err := s.repo.ListBillzSyncCandidates(ctx)
	if err != nil {
		return result, err
	}
	result.Candidates = len(candidates)
	if len(candidates) == 0 {
		return result, nil
	}

	mainByISBN := make(map[string][]repository.MainProductBillzSyncCandidate, len(candidates))
	for _, candidate := range candidates {
		normalized := candidate.ISBNNormalized
		if normalized == "" {
			normalized = normalizeBillzCode(candidate.ISBN)
		}
		if normalized == "" {
			continue
		}
		mainByISBN[normalized] = append(mainByISBN[normalized], candidate)
	}
	if len(mainByISBN) == 0 {
		return result, nil
	}

	updates := make([]repository.MainProductBillzSyncUpdate, 0)
	matchedCount := 0

	for barcode, billz := range billzByBarcode {
		matches := mainByISBN[barcode]
		if len(matches) == 0 {
			continue
		}

		for _, candidate := range matches {
			matchedCount++

			currentQty := 0.0
			if candidate.Quantity != nil {
				currentQty = *candidate.Quantity
			}

			currentPrice := 0.0
			if candidate.Price != nil {
				currentPrice = *candidate.Price
			}

			currentNormalized := candidate.ISBNNormalized
			if currentNormalized == "" {
				currentNormalized = normalizeBillzCode(candidate.ISBN)
			}

			needsUpdate := currentNormalized != barcode ||
				candidate.Quantity == nil || !floatEqual(currentQty, billz.Quantity) ||
				candidate.Price == nil || !floatEqual(currentPrice, billz.Price)
			if !needsUpdate {
				continue
			}

			updates = append(updates, repository.MainProductBillzSyncUpdate{
				ID:             candidate.ID,
				ISBNNormalized: barcode,
				Quantity:       billz.Quantity,
				Price:          billz.Price,
			})
		}
	}

	result.Matched = matchedCount
	if len(updates) == 0 {
		return result, nil
	}

	updated, err := s.repo.ApplyBillzSyncUpdates(ctx, updates)
	if err != nil {
		return result, err
	}
	result.Updated = updated

	return result, nil
}

func (s *BillzSyncService) login(ctx context.Context) (string, error) {
	authURL := strings.TrimRight(strings.TrimSpace(s.cfg.BillzAuthURL), "/")
	if authURL == "" {
		return "", fmt.Errorf("BILLZ_AUTH_URL is empty")
	}
	secret := strings.Trim(strings.TrimSpace(s.cfg.BillzAPISecret), `"'`)
	if secret == "" {
		return "", fmt.Errorf("BILLZ_API_SECRET_KEY is empty")
	}

	reqBody := map[string]string{
		"secret_token": secret,
	}

	var response billzLoginResponse
	if err := s.doJSON(ctx, http.MethodPost, authURL+"/login", reqBody, nil, &response); err != nil {
		return "", err
	}

	token := strings.TrimSpace(response.Data.AccessToken)
	if token == "" {
		token = strings.TrimSpace(response.AccessToken)
	}
	if token == "" {
		return "", fmt.Errorf("billz login succeeded but access token is empty")
	}
	return token, nil
}

func (s *BillzSyncService) fetchProducts(ctx context.Context, token string) ([]billzProduct, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(s.cfg.BillzURL), "/")
	if baseURL == "" {
		return nil, fmt.Errorf("BILLZ_URL is empty")
	}

	headers := map[string]string{
		"Authorization": "Bearer " + token,
	}

	endpoint := baseURL + "/products"
	baseProducts, expectedTotal, limitedByWindow, err := s.fetchProductsSegment(ctx, endpoint, headers, "")
	if err != nil {
		return nil, err
	}
	if !limitedByWindow || expectedTotal <= len(baseProducts) {
		return baseProducts, nil
	}

	unique := make(map[string]billzProduct, expectedTotal)
	added := addUniqueBillzProducts(unique, baseProducts)
	log.Printf(
		"billz products base window reached: fetched=%d expected=%d limit=%d maxWindow=%d; starting segmented backfill",
		added,
		expectedTotal,
		billzPageLimitForSearch(""),
		billzProductsMaxWindow,
	)

	segments := buildBillzInitialSegments()
	processedSegments := make(map[string]struct{}, len(segments))
	for i := 0; i < len(segments); i++ {
		if len(unique) >= expectedTotal {
			break
		}

		segment := segments[i]
		if _, alreadyProcessed := processedSegments[segment]; alreadyProcessed {
			continue
		}
		processedSegments[segment] = struct{}{}

		products, segmentCount, segmentLimited, err := s.fetchProductsSegment(ctx, endpoint, headers, segment)
		if err != nil {
			return nil, err
		}

		segmentAdded := addUniqueBillzProducts(unique, products)
		if segmentAdded > 0 && len(unique)%billzSegmentProgressLogStep < segmentAdded {
			log.Printf(
				"billz segmented backfill progress: fetched=%d expected=%d segment=%q segmentCount=%d segmentAdded=%d",
				len(unique),
				expectedTotal,
				segment,
				segmentCount,
				segmentAdded,
			)
		}

		if segmentLimited && canRefineDigitSegment(segment) {
			for _, child := range expandDigitSegment(segment) {
				if _, alreadyProcessed := processedSegments[child]; alreadyProcessed {
					continue
				}
				segments = append(segments, child)
			}
		}
	}

	if len(unique) < expectedTotal {
		return nil, fmt.Errorf(
			"billz segmented fetch incomplete: fetched=%d expected=%d (API window limit is %d)",
			len(unique),
			expectedTotal,
			billzProductsMaxWindow,
		)
	}

	result := make([]billzProduct, 0, len(unique))
	for _, product := range unique {
		result = append(result, product)
	}
	return result, nil
}

func (s *BillzSyncService) fetchProductsSegment(
	ctx context.Context,
	endpoint string,
	headers map[string]string,
	search string,
) ([]billzProduct, int, bool, error) {
	pageLimit := billzPageLimitForSearch(search)
	maxPages := billzProductsMaxWindow / pageLimit
	if maxPages <= 0 {
		maxPages = 1
	}

	collected := make([]billzProduct, 0)
	expectedTotal := 0
	limitedByWindow := false

	for page := 1; page <= maxPages; page++ {
		pageResponse, err := s.fetchProductsPage(ctx, endpoint, headers, search, page, pageLimit)
		if err != nil {
			return nil, 0, false, err
		}

		if page == 1 {
			expectedTotal = pageResponse.Count
			if expectedTotal > 0 {
				capHint := expectedTotal
				if capHint > billzProductsMaxWindow {
					capHint = billzProductsMaxWindow
				}
				collected = make([]billzProduct, 0, capHint)
			}
		}

		if len(pageResponse.Products) == 0 {
			break
		}
		collected = append(collected, pageResponse.Products...)

		if expectedTotal > 0 && len(collected) >= expectedTotal {
			break
		}
		if len(pageResponse.Products) < pageLimit {
			break
		}
	}

	if expectedTotal > len(collected) {
		limitedByWindow = true
	}
	return collected, expectedTotal, limitedByWindow, nil
}

func (s *BillzSyncService) fetchProductsPage(
	ctx context.Context,
	endpoint string,
	headers map[string]string,
	search string,
	page int,
	pageLimit int,
) (billzProductsResponse, error) {
	var response billzProductsResponse

	u, err := url.Parse(endpoint)
	if err != nil {
		return response, err
	}

	query := u.Query()
	query.Set("page", strconv.Itoa(page))
	query.Set("limit", strconv.Itoa(pageLimit))
	if strings.TrimSpace(search) != "" {
		query.Set("search", search)
	}
	u.RawQuery = query.Encode()

	if err := s.doJSON(ctx, http.MethodGet, u.String(), nil, headers, &response); err != nil {
		return response, err
	}
	return response, nil
}

func addUniqueBillzProducts(index map[string]billzProduct, products []billzProduct) int {
	added := 0
	for _, product := range products {
		key := billzProductKey(product)
		if key == "" {
			continue
		}
		if _, exists := index[key]; exists {
			continue
		}
		index[key] = product
		added++
	}
	return added
}

func billzProductKey(product billzProduct) string {
	id := strings.TrimSpace(product.ID)
	if id != "" {
		return "id:" + id
	}
	barcode := normalizeBillzCode(product.Barcode)
	if barcode != "" {
		return "barcode:" + barcode
	}
	return ""
}

func buildBillzInitialSegments() []string {
	segments := make([]string, 0, 100+10+26)
	for first := 0; first <= 9; first++ {
		for second := 0; second <= 9; second++ {
			segments = append(segments, fmt.Sprintf("%d%d", first, second))
		}
	}
	for digit := 0; digit <= 9; digit++ {
		segments = append(segments, strconv.Itoa(digit))
	}
	for r := 'A'; r <= 'Z'; r++ {
		segments = append(segments, string(r))
	}
	return segments
}

func canRefineDigitSegment(segment string) bool {
	if len(segment) == 0 || len(segment) >= billzSegmentMaxDepthDigits {
		return false
	}
	for _, r := range segment {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func expandDigitSegment(segment string) []string {
	expanded := make([]string, 0, 10)
	for digit := 0; digit <= 9; digit++ {
		expanded = append(expanded, segment+strconv.Itoa(digit))
	}
	return expanded
}

func (s *BillzSyncService) buildBarcodeMap(products []billzProduct) map[string]billzStockPrice {
	targetShop := strings.ToLower(strings.TrimSpace(s.cfg.BillzTargetShop))
	if targetShop == "" {
		targetShop = "topar"
	}

	index := make(map[string]billzStockPrice, len(products))
	for _, product := range products {
		barcode := normalizeBillzCode(product.Barcode)
		if barcode == "" {
			continue
		}

		quantity := 0.0
		for _, measurement := range product.ShopMeasurementValues {
			if !containsShop(measurement.ShopName, targetShop) {
				continue
			}
			if number, ok := firstNumber(measurement.ActiveMeasurementValue, measurement.ActiveMeasurementValueCamel); ok {
				quantity += number
			}
		}

		price := 0.0
		for _, shopPrice := range product.ShopPrices {
			if !containsShop(shopPrice.ShopName, targetShop) {
				continue
			}

			value, ok := firstNumber(shopPrice.RetailPrice, shopPrice.RetailPriceCamel)
			if !ok {
				continue
			}
			price = value
			if price > 0 {
				break
			}
		}

		existing, exists := index[barcode]
		if !exists || quantity > existing.Quantity || (floatEqual(quantity, existing.Quantity) && price > existing.Price) {
			index[barcode] = billzStockPrice{
				Quantity: quantity,
				Price:    price,
			}
		}
	}

	return index
}

func (s *BillzSyncService) doJSON(
	ctx context.Context,
	method string,
	endpoint string,
	body any,
	headers map[string]string,
	output any,
) error {
	var rawBody []byte
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rawBody = raw
	}

	var lastErr error
	maxAttempts := billzRequestMaxRetries + 1

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		var requestBody io.Reader
		if rawBody != nil {
			requestBody = bytes.NewReader(rawBody)
		}

		req, err := http.NewRequestWithContext(ctx, method, endpoint, requestBody)
		if err != nil {
			return err
		}
		if rawBody != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Accept", "application/json")
		for key, value := range headers {
			req.Header.Set(key, value)
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			lastErr = err
			if attempt < maxAttempts && shouldRetryBillzTransportErr(ctx, err) && waitBillzRetryDelay(ctx, attempt) {
				log.Printf("billz request retry attempt=%d/%d method=%s endpoint=%s error=%v", attempt, maxAttempts, method, endpoint, err)
				continue
			}
			return err
		}

		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			if attempt < maxAttempts && shouldRetryBillzTransportErr(ctx, readErr) && waitBillzRetryDelay(ctx, attempt) {
				log.Printf("billz response-read retry attempt=%d/%d method=%s endpoint=%s error=%v", attempt, maxAttempts, method, endpoint, readErr)
				continue
			}
			return readErr
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if output == nil || len(respBody) == 0 {
				return nil
			}
			if err := json.Unmarshal(respBody, output); err != nil {
				return err
			}
			return nil
		}

		message := strings.TrimSpace(string(respBody))
		if len(message) > 500 {
			message = message[:500] + "..."
		}
		lastErr = fmt.Errorf("billz request failed (%d): %s", resp.StatusCode, message)
		if attempt < maxAttempts && shouldRetryBillzStatus(resp.StatusCode) && waitBillzRetryDelay(ctx, attempt) {
			log.Printf("billz status retry attempt=%d/%d method=%s endpoint=%s status=%d", attempt, maxAttempts, method, endpoint, resp.StatusCode)
			continue
		}
		return lastErr
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("billz request failed: exhausted retries")
}

func shouldRetryBillzStatus(status int) bool {
	switch status {
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return status >= 500
	}
}

func shouldRetryBillzTransportErr(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	if errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EHOSTUNREACH) ||
		errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EADDRNOTAVAIL) {
		return true
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "can't assign requested address") ||
		strings.Contains(message, "client.timeout exceeded while awaiting headers") ||
		strings.Contains(message, "connection reset by peer") ||
		strings.Contains(message, "broken pipe") ||
		strings.Contains(message, "use of closed network connection") ||
		strings.Contains(message, "server closed idle connection")
}

func billzPageLimitForSearch(search string) int {
	if strings.TrimSpace(search) == "" {
		return billzProductsBasePageLimit
	}
	return billzProductsSegmentLimit
}

func waitBillzRetryDelay(ctx context.Context, attempt int) bool {
	if attempt <= 0 {
		attempt = 1
	}
	delay := billzRetryBaseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= billzRetryMaxDelay {
			delay = billzRetryMaxDelay
			break
		}
	}
	if delay > billzRetryMaxDelay {
		delay = billzRetryMaxDelay
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func containsShop(shopName string, target string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(shopName)), target)
}

func firstNumber(values ...any) (float64, bool) {
	for _, value := range values {
		if number, ok := parseNumber(value); ok {
			return number, true
		}
	}
	return 0, false
}

func parseNumber(value any) (float64, bool) {
	switch v := value.(type) {
	case nil:
		return 0, false
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func normalizeBillzCode(value string) string {
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

func floatEqual(a, b float64) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < 1e-6
}

func (s *BillzSyncService) startRun() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return false
	}
	s.running = true
	return true
}

func (s *BillzSyncService) finishRun() {
	s.mu.Lock()
	s.running = false
	s.mu.Unlock()
}
