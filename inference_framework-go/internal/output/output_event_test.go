package output

import (
	"encoding/json"
	"testing"
)

func TestNewEvent(t *testing.T) {
	event := NewEvent("test_user", 0.75, map[string]interface{}{"reason": "test"}, "test_model")

	if event.UserID != "test_user" {
		t.Errorf("Expected user_id 'test_user', got '%s'", event.UserID)
	}
	if event.Score < 0.0 || event.Score > 1.0 {
		t.Errorf("Expected score in [0, 1], got %f", event.Score)
	}
	if event.Score != 0.75 {
		t.Errorf("Expected score 0.75, got %f", event.Score)
	}
	if event.ModelID != "test_model" {
		t.Errorf("Expected model_id 'test_model', got '%s'", event.ModelID)
	}
	if event.EventID == "" {
		t.Error("Expected non-empty event_id")
	}
	if event.Timestamp == "" {
		t.Error("Expected non-empty timestamp")
	}
	if event.Data["reason"] != "test" {
		t.Errorf("Expected data reason 'test', got '%v'", event.Data["reason"])
	}
}

func TestNewEventDefaultModelID(t *testing.T) {
	event := NewEvent("user", 0.5, nil, "")
	if event.ModelID != "unknown_model" {
		t.Errorf("Expected default model_id 'unknown_model', got '%s'", event.ModelID)
	}
	if event.Data == nil {
		t.Error("Expected non-nil data map")
	}
}

func TestEventSerialization(t *testing.T) {
	event := NewEvent("user", 0.5, nil, "model")
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Failed to marshal event: %v", err)
	}

	var deserialized map[string]interface{}
	if err := json.Unmarshal(data, &deserialized); err != nil {
		t.Fatalf("Failed to unmarshal event: %v", err)
	}

	if deserialized["score"].(float64) != 0.5 {
		t.Errorf("Expected score 0.5, got %v", deserialized["score"])
	}
	if _, ok := deserialized["event_id"]; !ok {
		t.Error("Expected event_id in serialized output")
	}
}
