package processor

import (
	"inference_framework/internal/features"
	"inference_framework/internal/models"
	"inference_framework/internal/output"
	"log"
)

// LogProcessor processes batches of logs: validates, fetches features, and runs the single loaded model.
type LogProcessor struct {
	featureStore   *features.Store
	modelLoader    *models.Loader
	eventProcessor *EventProcessor
	model          models.Model
}

// NewLogProcessor creates a LogProcessor with the given feature store and model loader.
func NewLogProcessor(featureStore *features.Store, modelLoader *models.Loader) (*LogProcessor, error) {
	model := modelLoader.GetModel()
	if model == nil {
		log.Fatal("No model loaded by ModelLoader")
	}

	return &LogProcessor{
		featureStore:   featureStore,
		modelLoader:    modelLoader,
		eventProcessor: NewEventProcessor(),
		model:          model,
	}, nil
}

// ProcessBatch processes a validated batch: prepares messages, fetches features,
// runs inference via the single model, and returns output events.
func (lp *LogProcessor) ProcessBatch(validatedBatch []map[string]interface{}) []output.Event {
	if len(validatedBatch) == 0 {
		return nil
	}

	// Process batch through event processor
	messages := lp.eventProcessor.ProcessBatch(validatedBatch)
	if len(messages) == 0 {
		return nil
	}

	// Get unique user_ids
	userIDSet := make(map[string]struct{})
	for _, msg := range messages {
		if uid, ok := msg["user_id"].(string); ok && uid != "" {
			userIDSet[uid] = struct{}{}
		}
	}
	userIDs := make([]string, 0, len(userIDSet))
	for uid := range userIDSet {
		userIDs = append(userIDs, uid)
	}

	// Fetch features
	feats := lp.featureStore.GetFeaturesForUsers(userIDs)

	// Run the single model
	modelName := lp.modelLoader.GetModelName()
	var allEvents []output.Event

	modelEvents := lp.model.Process(messages, feats)
	allEvents = append(allEvents, modelEvents...)

	log.Printf("Generated %d inference events from batch using model: %s", len(allEvents), modelName)
	return allEvents
}
