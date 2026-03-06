package services

import (
  "io"
  "net/http"
  "testing"
)

func TestProbeExtractChitai(t *testing.T) {
  urls := []string{
    "https://www.chitai-gorod.ru/product/arhiv-buresveta-kn-5-veter-i-pravda-tom-1-3147339",
    "https://www.chitai-gorod.ru/catalog/books-18030",
  }
  cl := &http.Client{}
  for _, u := range urls {
    req, _ := http.NewRequest(http.MethodGet, u, nil)
    req.Header.Set("User-Agent", "Mozilla/5.0")
    resp, err := cl.Do(req)
    if err != nil {
      t.Fatalf("fetch err %s: %v", u, err)
    }
    b, _ := io.ReadAll(resp.Body)
    _ = resp.Body.Close()
    data, ok := extractProductData(u, b)
    t.Logf("url=%s status=%d len=%d ok=%v keys=%d title=%q price=%v", u, resp.StatusCode, len(b), ok, len(data), toString(data["title"]), data["price"])
  }
}
