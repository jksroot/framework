package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	yamlContent := `
kafka:
  output_topic: "test_output"
  dlq_topic: "test_dlq"
  bootstrap_servers: "localhost:9092"
  group_id: "test-group"
  batch_size: 50
consumers:
  num_instances: 8
  max_instances: 12
  use_partition_count: true
models:
  login_anomaly:
    topic: "test_topic"
    schema_name: "test_schema"
    threshold: 0.5
  data_exfil:
    topic: "exfil_topic"
    schema_name: "exfil_schema"
    threshold: 0.3
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	if cfg.OutputTopic() != "test_output" {
		t.Errorf("Expected output_topic 'test_output', got '%s'", cfg.OutputTopic())
	}
	if cfg.DLQTopic() != "test_dlq" {
		t.Errorf("Expected dlq_topic 'test_dlq', got '%s'", cfg.DLQTopic())
	}

	// Check consumer config
	if cfg.Consumers.NumInstances != 8 {
		t.Errorf("Expected num_instances 8, got %d", cfg.Consumers.NumInstances)
	}
	if cfg.Consumers.MaxInstances != 12 {
		t.Errorf("Expected max_instances 12, got %d", cfg.Consumers.MaxInstances)
	}
	if !cfg.Consumers.UsePartitionCount {
		t.Errorf("Expected use_partition_count true, got false")
	}

	mc, ok := cfg.GetModelConfig("login_anomaly")
	if !ok {
		t.Fatal("Expected login_anomaly model config to exist")
	}
	if mc.Topic != "test_topic" {
		t.Errorf("Expected topic 'test_topic', got '%s'", mc.Topic)
	}
	if mc.SchemaName != "test_schema" {
		t.Errorf("Expected schema_name 'test_schema', got '%s'", mc.SchemaName)
	}
}

func TestDefaultModelName(t *testing.T) {
	yamlContent := `
models:
  login_anomaly: {}
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	name := cfg.DefaultModelName()
	if name != "login_anomaly" {
		t.Errorf("Expected default model name 'login_anomaly', got '%s'", name)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := LoadConfig("/nonexistent/config.yaml")
	if err == nil {
		t.Error("Expected error for missing config file")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	yamlContent := `
models:
  test_model:
    topic: "t"
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Check defaults were applied before YAML override
	if cfg.Kafka.BootstrapServers != "localhost:9092" {
		t.Errorf("Expected default bootstrap_servers, got '%s'", cfg.Kafka.BootstrapServers)
	}
	if cfg.Kafka.GroupID != "inference-consumer" {
		t.Errorf("Expected default group_id, got '%s'", cfg.Kafka.GroupID)
	}

	// Check consumer defaults
	if cfg.Consumers.NumInstances != runtime.NumCPU() {
		t.Errorf("Expected default num_instances %d, got %d", runtime.NumCPU(), cfg.Consumers.NumInstances)
	}
	if cfg.Consumers.MaxInstances != 10 {
		t.Errorf("Expected default max_instances 10, got %d", cfg.Consumers.MaxInstances)
	}
	if !cfg.Consumers.UsePartitionCount {
		t.Errorf("Expected default use_partition_count true, got false")
	}
}

func TestConsumerConfigDefaultsWhenZero(t *testing.T) {
	yamlContent := `
consumers:
  num_instances: 0
  max_instances: 0
models:
  test_model:
    topic: "t"
`
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("Failed to write test config: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Zero values should be replaced with defaults
	if cfg.Consumers.NumInstances != runtime.NumCPU() {
		t.Errorf("Expected num_instances to default to %d, got %d", runtime.NumCPU(), cfg.Consumers.NumInstances)
	}
	if cfg.Consumers.MaxInstances != 10 {
		t.Errorf("Expected max_instances to default to 10, got %d", cfg.Consumers.MaxInstances)
	}
}
