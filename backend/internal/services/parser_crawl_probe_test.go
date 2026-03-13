package services

import (
	"context"
	"testing"
)

func TestProbeCrawlChitai(t *testing.T) {
	s := NewParserAppService(nil, nil, nil, nil)
	urls, err := s.discoverBySiteCrawl(context.Background(), "https://www.chitai-gorod.ru/catalog/books-18030", newHostRateLimiter(1.2), 120, 3, 120)
	if err != nil {
		t.Fatalf("crawl err: %v", err)
	}
	t.Logf("count=%d", len(urls))
	max := 30
	if len(urls) < max {
		max = len(urls)
	}
	for i := 0; i < max; i++ {
		t.Logf("%d %s score=%d", i+1, urls[i], scoreProductURL(urls[i]))
	}
}
