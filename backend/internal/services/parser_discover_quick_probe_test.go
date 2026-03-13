package services

import (
	"context"
	"testing"
)

func TestProbeDiscoverQuickChitai(t *testing.T) {
	s := NewParserAppService(nil, nil, nil, nil)
	urls, err := s.discoverCandidateURLs(context.Background(), "https://www.chitai-gorod.ru/catalog/books-18030", 5, 10, 4)
	if err != nil {
		t.Fatalf("discover err: %v", err)
	}
	if len(urls) == 0 {
		t.Fatalf("no urls discovered")
	}
	t.Logf("count=%d first=%s score=%d", len(urls), urls[0], scoreProductURL(urls[0]))
	if !isLikelyProductPath(urls[0]) {
		t.Fatalf("top url is not likely product: %s", urls[0])
	}
}
