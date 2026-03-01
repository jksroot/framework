# Real-Time Inference Framework

A pluggable Python framework for real-time inference using Kafka, Pydantic for schema validation, Polars for data processing, and configurable models.

## Features
- Kafka consumer with batch processing
- Dynamic Pydantic schema validation from schema files
- Feature vector loading from JSON per user_id
- Pluggable model interface for inference
- Config-driven, model-agnostic design
- Generates output events in fixed format

## Project Structure
```
inference_framework/
├── __init__.py
├── config.py
├── kafka_consumer.py
├── schema_validator.py
├── log_processor.py
├── event_processor.py
├── feature_store.py
├── model_interface.py
├── model_loader.py
├── main.py
├── models/
│   └── __init__.py
│   └── base_model.py
│   └── login_anomaly.py  # example
├── configs/
│   └── config.yaml
schemas/
│   └── app_log_type_login.json  # schema definition
features/
│   └── user_features.json  # sample features
```

## Setup
1. `pip install -r requirements.txt`
2. Models are **directly keyed by name** under `models:` in `configs/config.yaml` (each includes `topic`, `schema_name`, etc.).
   - Configure Kafka (general), schemas (per-topic), features.
3. Run with optional CLI: `python -m inference_framework.main --model-name login_anomaly`
   - Each model subscribes to its ONE topic; msgs conform to its schema. Supports single-model-per-deployment (e.g., separate K8s Deployments per model, each with pods in a shared consumer group).

## Docker
Build per-model: `docker build --build-arg MODEL_NAME=login_anomaly -t inference-login .`
Run: `docker run --env KAFKA_BOOTSTRAP=... inference-login`
See Dockerfile for ARG/ENV handling (each image/deployment loads ONLY its model + subscribes to its topic).

## Testing
Run unit tests + coverage: `pytest tests/ --cov=inference_framework --cov-report=term-missing`
Tests cover Config, OutputEvent, models, processors, etc. (mocks for Kafka).
