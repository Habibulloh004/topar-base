package handlers

import "testing"

func TestParseLocalSyncRequestSeparatesInvalidRecords(t *testing.T) {
	body := []byte(`{
		"runId": "run-123",
		"rules": {
			"eksmo.name": {"source": "title"},
			"main.name": {"source": "title"}
		},
		"records": [
			{
				"sourceUrl": "https://example.com/valid",
				"data": {"title": "Valid product", "price": 10}
			},
			{
				"sourceUrl": "https://example.com/bad-data",
				"data": "broken"
			},
			{
				"sourceUrl": "https://example.com/missing-data"
			}
		]
	}`)

	req, err := parseLocalSyncRequest(body)
	if err != nil {
		t.Fatalf("parseLocalSyncRequest returned error: %v", err)
	}

	if len(req.Records) != 1 {
		t.Fatalf("expected 1 valid record, got %d", len(req.Records))
	}
	if len(req.Invalid) != 2 {
		t.Fatalf("expected 2 invalid records, got %d", len(req.Invalid))
	}
	if req.Records[0].SourceURL != "https://example.com/valid" {
		t.Fatalf("unexpected source url for valid record: %q", req.Records[0].SourceURL)
	}
	if req.Invalid[0].Error == "" || req.Invalid[1].Error == "" {
		t.Fatalf("expected invalid records to keep parse errors")
	}
}

func TestParseLocalSyncRequestIgnoresBrokenRulesButKeepsRecords(t *testing.T) {
	body := []byte(`{
		"runId": "run-456",
		"rules": {
			"eksmo.name": {"source": ["title"]},
			"main.name": {"source": "title"},
			"main.price": "broken"
		},
		"records": [
			{
				"sourceUrl": "https://example.com/valid",
				"data": {"title": "Valid product"}
			},
			{
				"sourceUrl": "https://example.com/invalid",
				"data": 123
			}
		]
	}`)

	req, err := parseLocalSyncRequest(body)
	if err != nil {
		t.Fatalf("parseLocalSyncRequest returned error: %v", err)
	}

	if len(req.Rules) != 1 {
		t.Fatalf("expected 1 usable rule, got %d", len(req.Rules))
	}
	if req.Rules["main.name"].Source != "title" {
		t.Fatalf("unexpected parsed rule: %#v", req.Rules["main.name"])
	}
	if len(req.Records) != 1 {
		t.Fatalf("expected 1 valid record, got %d", len(req.Records))
	}
	if len(req.Invalid) != 1 {
		t.Fatalf("expected 1 invalid record, got %d", len(req.Invalid))
	}
}
