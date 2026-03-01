FROM python:3.10-slim

# ARG for model name per Docker image/build/deployment (e.g., different images or overrides per K8s deployment)
ARG MODEL_NAME=login_anomaly
ENV MODEL_NAME=${MODEL_NAME}

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY inference_framework/ ./inference_framework/
COPY configs/ ./configs/
COPY schemas/ ./schemas/
COPY features/ ./features/

# For Kafka in Docker, usually link to external, but for demo:
# Example build: docker build --build-arg MODEL_NAME=login_anomaly -t inference-login .
# Run: docker run -e KAFKA_BOOTSTRAP=... inference-login
# This ensures each model deployment (K8s pods in shared consumer group) loads ONLY its model.
CMD python -m inference_framework.main --model-name ${MODEL_NAME}
