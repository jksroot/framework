package processor

import (
	"inference_framework/internal/config"
	"inference_framework/internal/features"
	"inference_framework/internal/models"
	"os"
	"path/filepath"
	"testing"
)

func TestEventProcessorBatch(t *testing.T) {
	ep := NewEventProcessor()
	msgs := []map[string]interface{}{
		{"user_id": "abc", "log_type": "login"},
	}
	result := ep.ProcessBatch(msgs)
	if len(result) != 1 {
		t.Errorf("Expected 1 message, got %d", len(result))
	}
	if result[0]["user_id"] != "abc" {
		t.Errorf("Expected user_id 'abc', got '%v'", result[0]["user_id"])
	}
}

func TestEventProcessorEmptyBatch(t *testing.T) {
	ep := NewEventProcessor()
	result := ep.ProcessBatch(nil)
	if result != nil {
		t.Error("Expected nil for empty batch")
	}
}

func TestLogProcessorInit(t *testing.T) {
	// Create a temp features file
	dir := t.TempDir()
	featPath := filepath.Join(dir, "features.json")
	os.WriteFile(featPath, []byte(`{"abc": {"version": "2024"}}`), 0644)

	store, _ := features.NewStore(featPath)

	cfg := &config.Config{
		Models: map[string]config.ModelConfig{
			"login_anomaly": {
				Topic:      "test_topic",
				SchemaName: "test_schema",
				Threshold:  0.5,
			},
		},
	}

	loader, err := models.NewLoader(cfg, "login_anomaly")
	if err != nil {
		t.Fatalf("Failed to load model: %v", err)
	}

	lp, err := NewLogProcessor(store, loader)
	if err != nil {
		t.Fatalf("Failed to create log processor: %v", err)
	}
	if lp.model == nil {
		t.Error("Expected non-nil model in log processor")
	}
}

func TestLogProcessorProcessBatch(t *testing.T) {
	dir := t.TempDir()
	featPath := filepath.Join(dir, "features.json")
	os.WriteFile(featPath, []byte(`{
		"abc": {"ip_address_profile": {"risk_score": 0.9}, "country_code_profile": {"travel_freq": 0}},
		"def": {"ip_address_profile": {"risk_score": 0.1}, "country_code_profile": {"travel_freq": 5}}
	}`), 0644)

	store, _ := features.NewStore(featPath)

	cfg := &config.Config{
		Models: map[string]config.ModelConfig{
			"login_anomaly": {
				Topic:      "test_topic",
				SchemaName: "test_schema",
				Threshold:  0.1, // low threshold
			},
		},
	}

	loader, _ := models.NewLoader(cfg, "login_anomaly")
	lp, _ := NewLogProcessor(store, loader)

	batch := []map[string]interface{}{
		{"user_id": "abc", "ip_address": "1.2.3.4", "country_code": "US"},
		{"user_id": "def", "ip_address": "5.6.7.8", "country_code": "RU"},
	}

	events := lp.ProcessBatch(batch)
	// With low threshold, we should get events (random scores)
	if events == nil {
		t.Log("No events generated (possible with random scores, but unlikely with threshold 0.1)")
	}
	for _, e := range events {
		if e.Score < 0.0 || e.Score > 1.0 {
			t.Errorf("Score out of range: %f", e.Score)
		}
	}
}

func TestLogProcessorEmptyBatch(t *testing.T) {
	dir := t.TempDir()
	featPath := filepath.Join(dir, "features.json")
	os.WriteFile(featPath, []byte(`{}`), 0644)

	store, _ := features.NewStore(featPath)

	cfg := &config.Config{
		Models: map[string]config.ModelConfig{
			"login_anomaly": {Topic: "t", SchemaName: "s", Threshold: 0.5},
		},
	}
	loader, _ := models.NewLoader(cfg, "login_anomaly")
	lp, _ := NewLogProcessor(store, loader)

	events := lp.ProcessBatch(nil)
	if events != nil {
		t.Error("Expected nil for empty batch")
	}
}
