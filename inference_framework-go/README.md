# Real-Time Inference Framework

A pluggable Go framework for near-real-time inference using Kafka, dynamic JSON schema validation, and configurable models. Refactored from the original Python implementation for improved performance and deployment simplicity.

## Features
- **Kafka consumer** with batch processing (using [segmentio/kafka-go](https://github.com/segmentio/kafka-go))
- **Dynamic schema validation** from JSON schema definition files
- **Feature vector loading** from JSON per user_id
- **Pluggable model interface** for inference (registry-based model loading)
- **Config-driven**, model-agnostic design (YAML via [gopkg.in/yaml.v3](https://gopkg.in/yaml.v3))
- **Standard output events** with UUID generation ([google/uuid](https://github.com/google/uuid))
- **Kafka producer** with retry + dead-letter queue (DLQ) fallback
- **Graceful shutdown** via OS signal handling (SIGINT/SIGTERM)
- **Multi-stage Docker build** for minimal container images

## Project Structure
```
├── cmd/
│   └── inference/
│       └── main.go              # Entry point
├── internal/
│   ├── config/
│   │   ├── config.go            # YAML config loading & accessors
│   │   └── config_test.go
│   ├── schema/
│   │   ├── validator.go         # Dynamic JSON schema validation
│   │   └── validator_test.go
│   ├── features/
│   │   ├── feature_store.go     # Feature vector store (JSON)
│   │   └── feature_store_test.go
│   ├── models/
│   │   ├── model.go             # Model interface definition
│   │   ├── base_model.go        # Base model with common utilities
│   │   ├── login_anomaly.go     # Example: login anomaly detection model
│   │   ├── loader.go            # Registry-based model loader
│   │   └── models_test.go
│   ├── processor/
│   │   ├── event_processor.go   # Batch message processor
│   │   ├── log_processor.go     # Orchestrator: DF + features + model
│   │   └── processor_test.go
│   ├── kafka/
│   │   ├── consumer.go          # Kafka consumer with batch fetching
│   │   └── producer.go          # Kafka producer with retry + DLQ
│   └── output/
│       ├── output_event.go      # Standard output event struct
│       └── output_event_test.go
├── configs/
│   └── config.yaml              # Framework configuration
├── schemas/
│   ├── app_log_type_login.json  # Login event schema
│   └── app_log_type_exfil.json  # Data exfil event schema
├── features/
│   └── user_features.json       # Sample user feature vectors
├── go.mod
├── go.sum
├── Dockerfile
└── README.md
```

## Setup

### Prerequisites
- Go 1.19+ installed
- Access to a Kafka cluster

### Build
```bash
go build -o inference-framework ./cmd/inference
```

### Configure
Models are **directly keyed by name** under `models:` in `configs/config.yaml` (each includes `topic`, `schema_name`, `threshold`, etc.).
- Configure Kafka settings (bootstrap servers, consumer group, batch size, output/DLQ topics)
- Define schemas per model topic in `schemas/`
- Provide user feature vectors in `features/`

### Run
```bash
# Run with specific model (recommended for per-model K8s deployments)
./inference-framework --model-name login_anomaly

# Run with custom config path
./inference-framework --config /path/to/config.yaml --model-name login_anomaly
```

Each model subscribes to its ONE Kafka topic; messages conform to its schema. Supports single-model-per-deployment (e.g., separate K8s Deployments per model, each with pods in a shared consumer group).

## Adding New Models

1. Create a new file in `internal/models/` (e.g., `data_exfil.go`)
2. Implement the `Model` interface:
   ```go
   type Model interface {
       Process(messages []map[string]interface{}, features map[string]map[string]interface{}) []output.Event
   }
   ```
3. Register your model in `internal/models/loader.go`:
   ```go
   var registry = map[string]ModelFactory{
       "login_anomaly": func(cfg config.ModelConfig) Model { return NewLoginAnomaly(cfg) },
       "data_exfil":    func(cfg config.ModelConfig) Model { return NewDataExfil(cfg) },
   }
   ```
4. Add the model's config under `models:` in `configs/config.yaml`

## Docker
Build per-model image:
```bash
docker build --build-arg MODEL_NAME=login_anomaly -t inference-login .
```

Run:
```bash
docker run --env KAFKA_BOOTSTRAP=kafka:9092 inference-login
```

The Dockerfile uses a multi-stage build (Go builder → Alpine runtime) for minimal image size. Each image/deployment loads ONLY its model and subscribes to its topic.

## Testing
Run all tests:
```bash
go test ./... -v
```

Run tests with coverage:
```bash
go test ./... -v -cover -coverprofile=coverage.out
go tool cover -func=coverage.out
```

Tests cover: Config loading, OutputEvent creation/serialization, schema validation, feature store, model interface/loader, event processor, and log processor.

## Key Libraries
| Library | Purpose |
|---------|---------|
| [segmentio/kafka-go](https://github.com/segmentio/kafka-go) | Pure Go Kafka client (consumer & producer) |
| [gopkg.in/yaml.v3](https://gopkg.in/yaml.v3) | YAML configuration parsing |
| [google/uuid](https://github.com/google/uuid) | UUID generation for output events |
