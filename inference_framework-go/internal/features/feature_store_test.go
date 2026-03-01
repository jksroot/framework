package features

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewStore(t *testing.T) {
	dir := t.TempDir()
	featuresPath := filepath.Join(dir, "features.json")
	content := `{"abc": {"version": "2024", "ip_address_profile": {"risk_score": 0.1}}}`
	if err := os.WriteFile(featuresPath, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write test features: %v", err)
	}

	store, err := NewStore(featuresPath)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}

	feat := store.GetFeatureVector("abc")
	if feat == nil {
		t.Fatal("Expected feature vector for 'abc'")
	}
	if feat["version"] != "2024" {
		t.Errorf("Expected version '2024', got '%v'", feat["version"])
	}
}

func TestGetFeatureVectorNotFound(t *testing.T) {
	dir := t.TempDir()
	featuresPath := filepath.Join(dir, "features.json")
	content := `{"abc": {"version": "2024"}}`
	os.WriteFile(featuresPath, []byte(content), 0644)

	store, _ := NewStore(featuresPath)
	feat := store.GetFeatureVector("nonexistent")
	if feat != nil {
		t.Error("Expected nil for nonexistent user")
	}
}

func TestGetFeaturesForUsers(t *testing.T) {
	dir := t.TempDir()
	featuresPath := filepath.Join(dir, "features.json")
	content := `{"abc": {"version": "2024"}, "def": {"version": "2025"}}`
	os.WriteFile(featuresPath, []byte(content), 0644)

	store, _ := NewStore(featuresPath)
	feats := store.GetFeaturesForUsers([]string{"abc", "def", "unknown"})

	if len(feats) != 3 {
		t.Errorf("Expected 3 entries, got %d", len(feats))
	}
	if feats["abc"]["version"] != "2024" {
		t.Errorf("Expected version '2024' for abc")
	}
	if len(feats["unknown"]) != 0 {
		t.Errorf("Expected empty map for unknown user")
	}
}

func TestNewStoreFileNotFound(t *testing.T) {
	store, err := NewStore("/nonexistent/features.json")
	if err != nil {
		t.Fatalf("Expected no error (warning only), got: %v", err)
	}
	if store.GetFeatureVector("abc") != nil {
		t.Error("Expected nil from empty store")
	}
}
