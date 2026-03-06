package services

import (
  "context"
  "testing"
)

func TestProbeCrawlQuickChitai(t *testing.T) {
  s := NewParserAppService(nil, nil, nil)
  urls, err := s.discoverBySiteCrawl(context.Background(), "https://www.chitai-gorod.ru/catalog/books-18030", newHostRateLimiter(4), 40, 3, 20)
  if err != nil {
    t.Fatalf("crawl err: %v", err)
  }
  if len(urls) == 0 {
    t.Fatalf("no urls discovered")
  }
  t.Logf("count=%d first=%s", len(urls), urls[0])
  if !isLikelyProductPath(urls[0]) {
    t.Fatalf("first discovered url is not likely product: %s", urls[0])
  }
}
