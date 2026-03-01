package schema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func setupTestSchemas(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	loginSchema := SchemaDef{
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
	if err := os.WriteFile(filepath.Join(dir, "app_log_type_login.json"), data, 0644); err != nil {
		t.Fatalf("Failed to write test schema: %v", err)
	}
	return dir
}

func TestNewValidator(t *testing.T) {
	dir := setupTestSchemas(t)
	v, err := NewValidator(dir)
	if err != nil {
		t.Fatalf("Failed to create validator: %v", err)
	}
	if v.GetSchema("app_log_type_login") == nil {
		t.Error("Expected app_log_type_login schema to be loaded")
	}
}

func TestValidateMessageSuccess(t *testing.T) {
	dir := setupTestSchemas(t)
	v, _ := NewValidator(dir)

	msg := map[string]interface{}{
		"event_id":        "abc-123",
		"log_type":        "login",
		"ip_address":      "1.2.3.4",
		"event_timestamp": "2024-01-01T00:00:00",
		"user_id":         "user1",
		"user_agent":      "Mozilla/5.0",
		"country_code":    "US",
	}

	validated, err := v.ValidateMessage(msg, "app_log_type_login")
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	if validated["user_id"] != "user1" {
		t.Errorf("Expected user_id 'user1', got '%v'", validated["user_id"])
	}
}

func TestValidateMessageMissingRequired(t *testing.T) {
	dir := setupTestSchemas(t)
	v, _ := NewValidator(dir)

	msg := map[string]interface{}{
		"log_type": "login",
	}

	_, err := v.ValidateMessage(msg, "app_log_type_login")
	if err == nil {
		t.Error("Expected validation error for missing required fields")
	}
}

func TestValidateMessageSchemaNotFound(t *testing.T) {
	dir := setupTestSchemas(t)
	v, _ := NewValidator(dir)

	_, err := v.ValidateMessage(map[string]interface{}{}, "nonexistent")
	if err == nil {
		t.Error("Expected error for nonexistent schema")
	}
}

func TestValidateMessageOptionalFields(t *testing.T) {
	dir := setupTestSchemas(t)
	v, _ := NewValidator(dir)

	msg := map[string]interface{}{
		"event_id":        "abc-123",
		"log_type":        "login",
		"ip_address":      "1.2.3.4",
		"event_timestamp": "2024-01-01T00:00:00",
		"user_id":         "user1",
		// user_agent and country_code are optional
	}

	validated, err := v.ValidateMessage(msg, "app_log_type_login")
	if err != nil {
		t.Fatalf("Validation failed: %v", err)
	}
	if validated["user_agent"] != nil {
		t.Errorf("Expected nil for optional missing user_agent, got '%v'", validated["user_agent"])
	}
}

func TestValidatorNonexistentDir(t *testing.T) {
	v, err := NewValidator("/nonexistent/schemas")
	if err != nil {
		t.Fatalf("Expected no error for nonexistent dir, got: %v", err)
	}
	if len(v.schemas) != 0 {
		t.Error("Expected empty schemas for nonexistent dir")
	}
}
