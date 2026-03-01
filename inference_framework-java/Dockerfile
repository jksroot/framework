FROM maven:3.8.6-openjdk-11 AS builder

# ARG for model name (passed to Java app via ENV/arg)
ARG MODEL_NAME=login_anomaly
ENV MODEL_NAME=${MODEL_NAME}

WORKDIR /app

# Copy pom and source
COPY pom.xml .
COPY src/ ./src/
# Copy data dirs (configs, schemas, features stay at root for loading)
COPY configs/ ./configs/
COPY schemas/ ./schemas/
COPY features/ ./features/

# Build the uber-jar
RUN mvn clean package -DskipTests

# Runtime stage
FROM openjdk:11-jre-slim

ARG MODEL_NAME=login_anomaly
ENV MODEL_NAME=${MODEL_NAME}

WORKDIR /app

# Copy jar and data files
COPY --from=builder /app/target/inference-framework-1.0-SNAPSHOT.jar ./inference-framework.jar
COPY configs/ ./configs/
COPY schemas/ ./schemas/
COPY features/ ./features/

# For Kafka in Docker, external bootstrap usually via ENV override in config or props.
# Supports dynamic multithreading: queries topic partitions from brokers (AdminClient), threads = min(partitions, 10)
# or fallback to num_consumer_threads in config.yaml (or override).
# Each thread = dedicated KafkaConsumer (same group.id; Kafka assigns partitions).
# Example build: docker build --build-arg MODEL_NAME=login_anomaly -t inference-login-java .
# Run: docker run -e KAFKA_BOOTSTRAP_SERVERS=... -e MODEL_NAME=login_anomaly inference-login-java
# Note: Each image/deployment loads ONLY its model + subscribes to its topic (via CLI arg).
# Config can be overridden via env if needed, but file-based here.
CMD java -jar inference-framework.jar --model-name ${MODEL_NAME}
