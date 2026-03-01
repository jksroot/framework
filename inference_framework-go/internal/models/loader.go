package models

import (
	"fmt"
	"inference_framework/internal/config"
	"log"
)

// ModelFactory is a function that creates a Model from a ModelConfig.
type ModelFactory func(cfg config.ModelConfig) Model

// registry maps model names to their factory functions.
// New models should register themselves here.
var registry = map[string]ModelFactory{
	"login_anomaly": func(cfg config.ModelConfig) Model {
		return NewLoginAnomaly(cfg)
	},
}

// RegisterModel registers a model factory under the given name.
// This allows new models to be plugged in without modifying the loader.
func RegisterModel(name string, factory ModelFactory) {
	registry[name] = factory
}

// Loader loads exactly ONE model by name (per-deployment, e.g., K8s pod).
type Loader struct {
	model      Model
	modelName  string
	topic      string
	schemaName string
}

// NewLoader creates a Loader that loads the specified model (or the default).
func NewLoader(cfg *config.Config, modelName string) (*Loader, error) {
	if modelName == "" {
		modelName = cfg.DefaultModelName()
	}

	modelCfg, ok := cfg.GetModelConfig(modelName)
	if !ok {
		return nil, fmt.Errorf("model config not found for: %s", modelName)
	}

	factory, ok := registry[modelName]
	if !ok {
		return nil, fmt.Errorf("no registered model factory for: %s", modelName)
	}

	model := factory(modelCfg)

	topic := modelCfg.Topic
	if topic == "" {
		topic = "app_logs"
	}
	schemaName := modelCfg.SchemaName
	if schemaName == "" {
		schemaName = "app_log_type_login"
	}

	log.Printf("Loaded single model for this deployment: %s (topic=%s, schema=%s)", modelName, topic, schemaName)

	return &Loader{
		model:      model,
		modelName:  modelName,
		topic:      topic,
		schemaName: schemaName,
	}, nil
}

// GetModel returns the loaded model.
func (l *Loader) GetModel() Model {
	return l.model
}

// GetModelName returns the deployed model name.
func (l *Loader) GetModelName() string {
	return l.modelName
}

// GetTopic returns the topic this model subscribes to.
func (l *Loader) GetTopic() string {
	return l.topic
}

// GetSchemaName returns the schema for messages in this model's topic.
func (l *Loader) GetSchemaName() string {
	return l.schemaName
}
