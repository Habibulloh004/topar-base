package services

import (
  "io"
  "net/http"
  "testing"
)

func TestProbeSindbadLinks(t *testing.T) {
  u := "https://sindbadbooks.ru/"
  req, _ := http.NewRequest(http.MethodGet, u, nil)
  req.Header.Set("User-Agent", "Mozilla/5.0")
  resp, err := (&http.Client{}).Do(req)
  if err != nil { t.Fatalf("fetch err: %v", err) }
  b, _ := io.ReadAll(resp.Body)
  _ = resp.Body.Close()
  links := extractNavigableLinks(u, b, "sindbadbooks.ru")
  t.Logf("links=%d", len(links))
  max := 20
  if len(links) < max { max = len(links) }
  for i:=0;i<max;i++ { t.Logf("%d %s", i+1, links[i]) }
}
