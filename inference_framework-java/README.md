# Real-Time Inference Framework (Java + Kafka)

Refactored pluggable Java framework for real-time inference/detection using Kafka, Jackson for config/JSON, networknt json-schema-validator for schemas, kafka-clients for messaging, and configurable models. Python/Polars/Pydantic replaced with Java equivalents for better enterprise integration.

## Features
- Kafka consumer/producer with batch processing and DLQ/retry support (using kafka-clients)
- **Dynamic multithreaded Kafka consumers**: query partitions from brokers via AdminClient, spawn threads = min(partitions, 10) or fallback to config.num_consumer_threads if >10; Kafka assigns partitions to threads (shared group.id) for parallel consumption
- Dynamic JSON Schema validation from schema files (using networknt library)
- Feature vector loading from JSON per user_id (Jackson)
- Pluggable model interface for inference (reflection loading)
- Config-driven (YAML via Jackson), model-agnostic design
- Generates standard OutputEvent in fixed format
- Single-model-per-deployment support for scaling (e.g., K8s)

## Project Structure
```
.
├── pom.xml                  # Maven build with deps: kafka-clients, jackson, json-schema-validator, etc.
├── src/
│   ├── main/java/com/inferenceframework/  # Modularized by functionality
│   │   ├── Main.java
│   │   ├── config/Config.java
│   │   ├── kafka/KafkaConsumer.java, KafkaProducer.java
│   │   ├── schema/SchemaValidator.java
│   │   ├── processing/EventProcessor.java, LogProcessor.java, FeatureStore.java
│   │   ├── model/ModelInterface.java, ModelLoader.java, models/ (BaseModel.java, LoginAnomaly.java)
│   │   └── event/OutputEvent.java
│   └── main/resources/logback.xml  # Logging config
├── configs/config.yaml      # (updated comments)
├── schemas/*.json           # Schemas (custom converted to JSON Schema internally)
├── features/user_features.json
├── Dockerfile               # Multi-stage Maven build
└── archive/python_original/ # Original Python code archived
```

Modular packages follow similar functionality for better organization (e.g., config/, kafka/, etc.). Imports updated accordingly.

## Setup
1. Ensure Java 11+, Maven.
2. `mvn clean compile` (downloads deps: Kafka, Jackson, etc.)
3. Models **directly keyed by name** under `models:` in `configs/config.yaml` (topic, schema_name, etc.).
   - Kafka, schemas, features configured.
4. Run with optional CLI: `mvn exec:java -Dexec.mainClass="com.inferenceframework.Main" -Dexec.args="--model-name login_anomaly"`
   OR `java -jar target/inference-framework-1.0-SNAPSHOT.jar --model-name login_anomaly`
   - Dynamic multithreaded flow: queries topic partitions from brokers (AdminClient), threads = min(partitions, 10) or fallback to `num_consumer_threads` in kafka config (each thread = dedicated KafkaConsumer; same group.id).
   - Kafka broker assigns partitions to threads for parallel consumption. Each model subscribes to its ONE topic; msgs conform to its schema. Supports single-model-per-deployment (separate K8s Deployments, shared consumer group via group.id).

## Docker
Build per-model: `docker build --build-arg MODEL_NAME=login_anomaly -t inference-login-java .`
Run: `docker run -e KAFKA_BOOTSTRAP_SERVERS=... inference-login-java`
See Dockerfile for multi-stage Maven build, ARG/ENV (each deployment loads ONLY its model + subscribes to its topic).

## Testing
Basic verification via code structure/tests can be added with JUnit (deps in pom).
Run: `mvn test`
Original Python tests archived; Java tests (mocks for Kafka) can be extended in src/test.
See Config, OutputEvent, models, processors (Kafka mocks recommended). Multi-consumer threads verifiable via rebalance logs + config `num_consumer_threads`.
