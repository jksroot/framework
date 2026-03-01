// Package main is the entry point for the Real-Time Inference Framework.
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"inference_framework/internal/config"
	"inference_framework/internal/features"
	"inference_framework/internal/kafka"
	"inference_framework/internal/models"
	"inference_framework/internal/schema"
)

func main() {
	// Parse CLI flags
	modelName := flag.String("model-name", "", "Model name to load (must match key under models: in config; e.g., login_anomaly)")
	configPath := flag.String("config", "configs/config.yaml", "Path to config YAML file")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Setup logging
	setupLogging(cfg)
	log.Println("Starting Real-Time Inference Framework")

	// Initialize schema validator
	schemaValidator, err := schema.NewValidator(cfg.Schemas.BasePath)
	if err != nil {
		log.Fatalf("Failed to initialize schema validator: %v", err)
	}

	// Initialize feature store
	featureStore, err := features.NewStore(cfg.Features.Path)
	if err != nil {
		log.Fatalf("Failed to initialize feature store: %v", err)
	}

	// Load model (pass model-name from CLI for single-model per deployment)
	modelLoader, err := models.NewLoader(cfg, *modelName)
	if err != nil {
		log.Fatalf("Failed to load model: %v", err)
	}

	// Initialize Kafka producer first (shared by all consumer instances)
	producer := kafka.NewProducer(cfg)
	defer producer.Close()

	// Initialize ConsumerManager with multiple consumer instances
	// Each instance is an independent Kafka consumer in the same consumer group
	consumerManager := kafka.NewConsumerManager(cfg, schemaValidator, featureStore, modelLoader, producer)

	// Setup context with signal handling for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
		consumerManager.Stop()
	}()

	log.Printf("Framework initialized with model: %s (topic=%s, schema=%s, consumers=%d). Starting consumers...",
		modelLoader.GetModelName(), modelLoader.GetTopic(), modelLoader.GetSchemaName(), cfg.Consumers.NumInstances)

	// Start all consumer instances
	consumerManager.Start()

	// Wait for shutdown signal
	<-ctx.Done()
	log.Println("Shutdown complete")
}

// setupLogging configures the log output to both stderr and a log file.
func setupLogging(cfg *config.Config) {
	logFile := cfg.Logging.File
	if logFile == "" {
		logFile = "inference.log"
	}

	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("WARNING: Could not open log file %s: %v", logFile, err)
		return
	}

	// Write to both stderr and file
	mw := io.MultiWriter(os.Stderr, f)
	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}
