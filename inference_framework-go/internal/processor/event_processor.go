// Package processor handles batch processing of validated messages.
package processor

import (
	"log"
)

// EventProcessor converts batches of validated messages for model consumption.
// In Go, we work directly with []map[string]interface{} instead of DataFrames.
type EventProcessor struct{}

// NewEventProcessor creates a new EventProcessor.
func NewEventProcessor() *EventProcessor {
	return &EventProcessor{}
}

// ProcessBatch takes a list of validated message maps and returns them ready for model processing.
// In the Python version this created a Polars DataFrame; in Go we pass maps directly.
func (ep *EventProcessor) ProcessBatch(validatedMessages []map[string]interface{}) []map[string]interface{} {
	if len(validatedMessages) == 0 {
		return nil
	}
	log.Printf("Created batch with %d messages", len(validatedMessages))
	return validatedMessages
}
