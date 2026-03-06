package services

import (
  "context"
  "testing"
)

func TestProbeDiscoverChitaiAfterFix(t *testing.T) {
  s := NewParserAppService(nil, nil, nil)
  urls, err := s.discoverCandidateURLs(context.Background(), "https://www.chitai-gorod.ru/catalog/books-18030", 20, 40, 1.2)
  if err != nil {
    t.Fatalf("discover err: %v", err)
  }
  t.Logf("count=%d", len(urls))
  productLike := 0
  max := 20
  if len(urls) < max {
    max = len(urls)
  }
  for i := 0; i < max; i++ {
    if isLikelyProductPath(urls[i]) || scoreProductURL(urls[i]) >= 18 {
      productLike++
    }
    t.Logf("%d %s score=%d", i+1, urls[i], scoreProductURL(urls[i]))
  }
  if productLike == 0 {
    t.Fatalf("no likely products in top %d urls", max)
  }
}
