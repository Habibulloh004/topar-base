package services

import (
  "io"
  "net/http"
  "testing"
)

func TestProbeSindbadProduct(t *testing.T) {
  u := "https://sindbadbooks.ru/index.php?route=product/product&path=25&product_id=413"
  req, _ := http.NewRequest(http.MethodGet, u, nil)
  req.Header.Set("User-Agent", "Mozilla/5.0")
  resp, err := (&http.Client{}).Do(req)
  if err != nil { t.Fatalf("fetch err: %v", err) }
  b, _ := io.ReadAll(resp.Body)
  _ = resp.Body.Close()
  data, ok := extractProductData(u, b)
  t.Logf("status=%d len=%d ok=%v keys=%d title=%q price=%v isbn=%q availability=%q", resp.StatusCode, len(b), ok, len(data), toString(data["title"]), data["price"], toString(data["isbn"]), toString(data["availability"]))
  if !ok { t.Fatalf("not detected as product") }
}
