package models

import (
	"inference_framework/internal/config"
	"inference_framework/internal/output"
	"testing"
)

func TestLoginAnomalyProcess(t *testing.T) {
	cfg := config.ModelConfig{Threshold: 0.1} // low threshold to catch random scores
	model := NewLoginAnomaly(cfg)

	messages := []map[string]interface{}{
		{"user_id": "abc", "ip_address": "1.2.3.4"},
		{"user_id": "def", "ip_address": "5.6.7.8"},
	}
	features := map[string]map[string]interface{}{
		"abc": {"ip_address_profile": map[string]interface{}{"risk_score": 0.9}},
		"def": {"ip_address_profile": map[string]interface{}{"risk_score": 0.1}},
	}

	events := model.Process(messages, features)
	if len(events) == 0 {
		t.Error("Expected at least some events with low threshold")
	}
	for _, e := range events {
		if e.Score < 0.0 || e.Score > 1.0 {
			t.Errorf("Expected score in [0, 1], got %f", e.Score)
		}
		if e.UserID == "" {
			t.Error("Expected non-empty user_id")
		}
	}
}

func TestLoginAnomalySkipsMissingUser(t *testing.T) {
	cfg := config.ModelConfig{Threshold: 0.0}
	model := NewLoginAnomaly(cfg)

	messages := []map[string]interface{}{
		{"user_id": "unknown", "ip_address": "1.2.3.4"},
	}
	features := map[string]map[string]interface{}{
		"abc": {"ip_address_profile": map[string]interface{}{"risk_score": 0.5}},
	}

	events := model.Process(messages, features)
	if len(events) != 0 {
		t.Errorf("Expected 0 events for unknown user, got %d", len(events))
	}
}

func TestBaseModelProcess(t *testing.T) {
	base := &BaseModel{}
	events := base.Process(nil, nil)
	if events != nil {
		t.Error("Expected nil from BaseModel.Process")
	}
}

func TestModelLoaderRegistry(t *testing.T) {
	cfg := &config.Config{
		Models: map[string]config.ModelConfig{
			"login_anomaly": {
				Topic:      "test_topic",
				SchemaName: "test_schema",
				Threshold:  0.5,
			},
		},
	}

	loader, err := NewLoader(cfg, "login_anomaly")
	if err != nil {
		t.Fatalf("Failed to load model: %v", err)
	}
	if loader.GetModelName() != "login_anomaly" {
		t.Errorf("Expected model name 'login_anomaly', got '%s'", loader.GetModelName())
	}
	if loader.GetTopic() != "test_topic" {
		t.Errorf("Expected topic 'test_topic', got '%s'", loader.GetTopic())
	}
	if loader.GetSchemaName() != "test_schema" {
		t.Errorf("Expected schema 'test_schema', got '%s'", loader.GetSchemaName())
	}
	if loader.GetModel() == nil {
		t.Error("Expected non-nil model")
	}
}

func TestModelLoaderUnknownModel(t *testing.T) {
	cfg := &config.Config{
		Models: map[string]config.ModelConfig{
			"unknown_model": {Topic: "t"},
		},
	}

	_, err := NewLoader(cfg, "unknown_model")
	if err == nil {
		t.Error("Expected error for unregistered model")
	}
}

func TestGenerateOutputEvent(t *testing.T) {
	event := GenerateOutputEvent("user1", 0.8, map[string]interface{}{"key": "val"}, "model1")
	if event.UserID != "user1" {
		t.Errorf("Expected user_id 'user1', got '%s'", event.UserID)
	}
	if event.Score != 0.8 {
		t.Errorf("Expected score 0.8, got %f", event.Score)
	}
	var _ output.Event = event // type check
}
