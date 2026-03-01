package models

import (
	"inference_framework/internal/config"
	"inference_framework/internal/output"
	"log"
)

// BaseModel provides common utilities for all models.
type BaseModel struct {
	Config  config.ModelConfig
	ModelID string
}

// NewBaseModel creates a new BaseModel with the given config and model ID.
func NewBaseModel(cfg config.ModelConfig, modelID string) BaseModel {
	return BaseModel{
		Config:  cfg,
		ModelID: modelID,
	}
}

// Process is the default implementation — should be overridden by embedding structs.
func (b *BaseModel) Process(messages []map[string]interface{}, features map[string]map[string]interface{}) []output.Event {
	log.Printf("WARNING: BaseModel.Process called — override in subtype")
	return nil
}
