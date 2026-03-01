package processor

import (
	"encoding/json"
	"inference_framework/internal/config"
	"inference_framework/internal/features"
	"inference_framework/internal/models"
	"inference_framework/internal/schema"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func setupTestDependencies(t *testing.T) (*schema.Validator, *features.Store, *models.Loader) {
	t.Helper()

	// Create temp schema
	dir := t.TempDir()
	schemaDir := filepath.Join(dir, "schemas")
	os.MkdirAll(schemaDir, 0755)

	loginSchema := schema.SchemaDef{
		Name: "AppLogLogin",
		Fields: map[string]string{
			"event_id":        "str",
			"log_type":        "str",
			"ip_address":      "str",
			"event_timestamp": "str",
			"user_id":         "str",
			"user_agent":      "str",
			"country_code":    "str",
		},
		Required: []string{"event_id", "log_type", "ip_address", "event_timestamp", "user_id"},
	}
	data, _ := json.Marshal(loginSchema)
	os.WriteFile(filepath.Join(schemaDir, "app_log_type_login.json"), data, 0644)

	validator, err := schema.NewValidator(schemaDir)
	if err != nil {
		t.Fatalf("Failed to create validator: %v", err)
	}

	// Create temp features
	featPath := filepath.Join(dir, "features.json")
	os.WriteFile(featPath, []byte(`{
		"abc": {"ip_address_profile": {"risk_score": 0.9}, "country_code_profile": {"travel_freq": 0}},
		"def": {"ip_address_profile": {"risk_score": 0.1}, "country_code_profile": {"travel_freq": 5}}
	}`), 0644)

	store, _ := features.NewStore(featPath)

	// Create model loader
	cfg := &config.Config{
		Models: map[string]config.ModelConfig{
			"login_anomaly": {
				Topic:      "test_topic",
				SchemaName: "app_log_type_login",
				Threshold:  0.1, // Low threshold for testing
			},
		},
	}
	loader, err := models.NewLoader(cfg, "login_anomaly")
	if err != nil {
		t.Fatalf("Failed to load model: %v", err)
	}

	return validator, store, loader
}

func TestConcurrentProcessorStartStop(t *testing.T) {
	validator, store, loader := setupTestDependencies(t)
	lp, _ := NewLogProcessor(store, loader)

	cp := NewConcurrentProcessor(2, 10, 10, validator, lp, "app_log_type_login")
	
	cp.Start()
	time.Sleep(100 * time.Millisecond) // Let workers start
	cp.Stop()
}

func TestConcurrentProcessorSubmitAndProcess(t *testing.T) {
	validator, store, loader := setupTestDependencies(t)
	lp, _ := NewLogProcessor(store, loader)

	cp := NewConcurrentProcessor(2, 10, 10, validator, lp, "app_log_type_login")
	cp.Start()
	defer cp.Stop()

	// Submit work items
	item := WorkItem{
		BatchID: 1,
		Messages: []map[string]interface{}{
			{"event_id": "1", "log_type": "login", "ip_address": "1.2.3.4", "event_timestamp": "2024-01-01", "user_id": "abc"},
			{"event_id": "2", "log_type": "login", "ip_address": "5.6.7.8", "event_timestamp": "2024-01-01", "user_id": "def"},
		},
	}

	if !cp.Submit(item) {
		t.Fatal("Failed to submit work item")
	}

	// Wait for and check result
	select {
	case result := <-cp.Results():
		if result.BatchID != 1 {
			t.Errorf("Expected batch ID 1, got %d", result.BatchID)
		}
		// With low threshold, we should get events
		t.Logf("Got %d events from batch", len(result.Events))
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for result")
	}
}

func TestConcurrentProcessorMultipleBatches(t *testing.T) {
	validator, store, loader := setupTestDependencies(t)
	lp, _ := NewLogProcessor(store, loader)

	cp := NewConcurrentProcessor(4, 20, 20, validator, lp, "app_log_type_login")
	cp.Start()
	defer cp.Stop()

	numBatches := 5
	
	// Submit multiple batches
	for i := 1; i <= numBatches; i++ {
		item := WorkItem{
			BatchID: i,
			Messages: []map[string]interface{}{
				{"event_id": string(rune('0' + i)), "log_type": "login", "ip_address": "1.2.3.4", "event_timestamp": "2024-01-01", "user_id": "abc"},
			},
		}
		if !cp.Submit(item) {
			t.Fatalf("Failed to submit batch %d", i)
		}
	}

	// Collect all results
	results := make(map[int]bool)
	timeout := time.After(5 * time.Second)
	
	for len(results) < numBatches {
		select {
		case result := <-cp.Results():
			results[result.BatchID] = true
			if result.Error != nil {
				t.Errorf("Batch %d had error: %v", result.BatchID, result.Error)
			}
		case <-timeout:
			t.Fatalf("Timeout waiting for results, got %d/%d", len(results), numBatches)
		}
	}

	if len(results) != numBatches {
		t.Errorf("Expected %d results, got %d", numBatches, len(results))
	}
}

func TestConcurrentProcessorInvalidMessage(t *testing.T) {
	validator, store, loader := setupTestDependencies(t)
	lp, _ := NewLogProcessor(store, loader)

	cp := NewConcurrentProcessor(2, 10, 10, validator, lp, "app_log_type_login")
	cp.Start()
	defer cp.Stop()

	// Submit invalid message (missing required fields)
	item := WorkItem{
		BatchID: 1,
		Messages: []map[string]interface{}{
			{"log_type": "login"}, // Missing required fields
		},
	}

	if !cp.Submit(item) {
		t.Fatal("Failed to submit work item")
	}

	// Should get empty result (all messages dropped)
	select {
	case result := <-cp.Results():
		if len(result.Events) != 0 {
			t.Errorf("Expected 0 events for invalid message, got %d", len(result.Events))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for result")
	}
}

func TestConcurrentProcessorSubmitAfterStop(t *testing.T) {
	validator, store, loader := setupTestDependencies(t)
	lp, _ := NewLogProcessor(store, loader)

	cp := NewConcurrentProcessor(1, 10, 10, validator, lp, "app_log_type_login")
	cp.Start()
	cp.Stop()

	// Try to submit after stop
	item := WorkItem{
		BatchID: 1,
		Messages: []map[string]interface{}{
			{"event_id": "1", "log_type": "login", "ip_address": "1.2.3.4", "event_timestamp": "2024-01-01", "user_id": "abc"},
		},
	}

	if cp.Submit(item) {
		t.Error("Expected Submit to return false after Stop")
	}
}