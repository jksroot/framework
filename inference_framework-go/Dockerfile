# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

# Copy go module files first for caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /inference-framework ./cmd/inference

# Runtime stage
FROM alpine:3.19

# ARG for model name per Docker image/build/deployment (e.g., different images or overrides per K8s deployment)
ARG MODEL_NAME=login_anomaly
ENV MODEL_NAME=${MODEL_NAME}

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /inference-framework /app/inference-framework

# Copy config and data files
COPY configs/ ./configs/
COPY schemas/ ./schemas/
COPY features/ ./features/

# For Kafka in Docker, usually link to external, but for demo:
# Example build: docker build --build-arg MODEL_NAME=login_anomaly -t inference-login .
# Run: docker run -e KAFKA_BOOTSTRAP=... inference-login
# This ensures each model deployment (K8s pods in shared consumer group) loads ONLY its model.
CMD /app/inference-framework --model-name ${MODEL_NAME}
