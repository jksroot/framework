// Package config provides configuration management for the inference framework.
package config

import (
	"fmt"
	"os"
	"runtime"

	"gopkg.in/yaml.v3"
)

// KafkaConfig holds Kafka-related settings.
type KafkaConfig struct {
	BootstrapServers string  `yaml:"bootstrap_servers"`
	GroupID          string  `yaml:"group_id"`
	AutoOffsetReset  string  `yaml:"auto_offset_reset"`
	EnableAutoCommit bool    `yaml:"enable_auto_commit"`
	BatchSize        int     `yaml:"batch_size"`
	PollTimeout      float64 `yaml:"poll_timeout"`
	OutputTopic      string  `yaml:"output_topic"`
	DLQTopic         string  `yaml:"dlq_topic"`
}

// ConsumerConfig holds settings for multiple Kafka consumer instances.
type ConsumerConfig struct {
	NumInstances      int  `yaml:"num_instances"`       // Number of consumer instances (0 = CPU count)
	MaxInstances      int  `yaml:"max_instances"`       // Max consumers when using partition count (fallback if partitions > this)
	UsePartitionCount bool `yaml:"use_partition_count"` // If true, spawn consumers = min(partition count, MaxInstances)
}

// SchemaConfig holds schema-related settings.
type SchemaConfig struct {
	BasePath string `yaml:"base_path"`
}

// FeaturesConfig holds features-related settings.
type FeaturesConfig struct {
	Path string `yaml:"path"`
}

// ModelConfig holds per-model configuration.
type ModelConfig struct {
	Topic        string                 `yaml:"topic"`
	SchemaName   string                 `yaml:"schema_name"`
	Threshold    float64                `yaml:"threshold"`
	AnomalyTypes []string              `yaml:"anomaly_types"`
	ModelParams  map[string]interface{} `yaml:"model_params"`
}

// LoggingConfig holds logging settings.
type LoggingConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// Config is the top-level configuration structure.
type Config struct {
	Kafka     KafkaConfig            `yaml:"kafka"`
	Consumers ConsumerConfig         `yaml:"consumers"`
	Schemas   SchemaConfig           `yaml:"schemas"`
	Features  FeaturesConfig         `yaml:"features"`
	Models    map[string]ModelConfig `yaml:"models"`
	Logging   LoggingConfig          `yaml:"logging"`
}

// LoadConfig loads configuration from a YAML file.
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config file not found: %s: %w", path, err)
	}

	cfg := &Config{
		// Defaults
		Kafka: KafkaConfig{
			BootstrapServers: "localhost:9092",
			GroupID:          "inference-consumer",
			AutoOffsetReset:  "earliest",
			EnableAutoCommit: false,
			BatchSize:        100,
			PollTimeout:      1.0,
			OutputTopic:      "inference_events",
			DLQTopic:         "inference_events_dlq",
		},
		Consumers: ConsumerConfig{
			NumInstances:      runtime.NumCPU(), // Default to CPU count
			MaxInstances:      10,               // Default max fallback
			UsePartitionCount: true,             // Default to using partition count
		},
		Schemas: SchemaConfig{
			BasePath: "schemas",
		},
		Features: FeaturesConfig{
			Path: "features/user_features.json",
		},
		Logging: LoggingConfig{
			Level: "INFO",
			File:  "inference.log",
		},
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	// Apply defaults for consumer config if not set
	if cfg.Consumers.NumInstances <= 0 {
		cfg.Consumers.NumInstances = runtime.NumCPU()
	}
	if cfg.Consumers.MaxInstances <= 0 {
		cfg.Consumers.MaxInstances = 10
	}

	return cfg, nil
}

// GetModelConfig returns the configuration for a specific model by name.
func (c *Config) GetModelConfig(modelName string) (ModelConfig, bool) {
	mc, ok := c.Models[modelName]
	return mc, ok
}

// DefaultModelName returns the first model name in the config as a fallback.
func (c *Config) DefaultModelName() string {
	for name := range c.Models {
		return name
	}
	return "login_anomaly"
}

// OutputTopic returns the common output topic.
func (c *Config) OutputTopic() string {
	if c.Kafka.OutputTopic != "" {
		return c.Kafka.OutputTopic
	}
	return "inference_events"
}

// DLQTopic returns the dead-letter queue topic.
func (c *Config) DLQTopic() string {
	if c.Kafka.DLQTopic != "" {
		return c.Kafka.DLQTopic
	}
	return "inference_events_dlq"
}
