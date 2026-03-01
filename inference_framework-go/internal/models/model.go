// Package models defines the model interface and model implementations for inference.
package models

import (
	"inference_framework/internal/output"
)

// Model is the interface that all inference models must implement.
type Model interface {
	// Process takes a batch of validated message maps and corresponding feature vectors,
	// and returns a list of output events.
	Process(messages []map[string]interface{}, features map[string]map[string]interface{}) []output.Event
}

// GenerateOutputEvent is a helper for models to create a standard OutputEvent.
func GenerateOutputEvent(userID string, score float64, data map[string]interface{}, modelID string) output.Event {
	return output.NewEvent(userID, score, data, modelID)
}
