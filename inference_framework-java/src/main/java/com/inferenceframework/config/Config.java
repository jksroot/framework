package com.inferenceframework.config;  // config module

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration manager for the Java inference framework.
 * Loads YAML config using Jackson.
 */
public class Config {
    private static final Logger logger = LoggerFactory.getLogger(Config.class);

    private final String configPath;
    private Map<String, Object> config;

    /**
     * Default constructor using standard config path.
     */
    public Config() {
        this("configs/config.yaml");
    }

    /**
     * Constructor with custom config path.
     */
    public Config(String configPath) {
        this.configPath = configPath;
        this.config = loadConfig();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadConfig() {
        // Load YAML configuration using Jackson.
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        File configFile = new File(configPath);
        if (!configFile.exists()) {
            throw new RuntimeException("Config file not found: " + configPath);
        }
        try {
            return mapper.readValue(configFile, Map.class);
        } catch (IOException e) {
            logger.error("Failed to load config", e);
            throw new RuntimeException("Config loading failed", e);
        }
    }

    /**
     * Get Kafka configuration (incl. multithreading opts like num_consumer_threads).
     * Each thread gets its own KafkaConsumer (same group.id for partition assignment by broker).
     * Supports dynamic: threads = min(topic partitions, maxThreadsCap=10) or config fallback.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getKafkaConfig() {
        return (Map<String, Object>) config.getOrDefault("kafka", new HashMap<>());
    }

    /**
     * Common Kafka topic for output events from ALL models.
     */
    public String getOutputTopic() {
        return (String) getKafkaConfig().getOrDefault("output_topic", "inference_events");
    }

    /**
     * Dead-letter queue topic for events that fail after retries.
     */
    public String getDlqTopic() {
        return (String) getKafkaConfig().getOrDefault("dlq_topic", "inference_events_dlq");
    }

    /**
     * Get schema configuration.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getSchemaConfig() {
        return (Map<String, Object>) config.getOrDefault("schemas", new HashMap<>());
    }

    /**
     * Get features JSON path.
     */
    @SuppressWarnings("unchecked")
    public String getFeaturesPath() {
        Map<String, Object> features = (Map<String, Object>) config.getOrDefault("features", new HashMap<>());
        return (String) features.getOrDefault("path", "features/user_features.json");
    }

    /**
     * Get all models (keyed by model_name under 'models:').
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getModelConfigs() {
        return (Map<String, Object>) config.getOrDefault("models", new HashMap<>());
    }

    /**
     * Get config for a specific model.
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getModelConfig(String modelName) {
        return (Map<String, Object>) getModelConfigs().getOrDefault(modelName, new HashMap<>());
    }

    /**
     * Fallback default model (first key in models dict).
     */
    public String getDefaultModelName() {
        Map<String, Object> models = getModelConfigs();
        if (!models.isEmpty()) {
            return models.keySet().iterator().next();
        }
        return "login_anomaly";
    }

    /**
     * Get logging level.
     */
    public String getLogLevel() {
        Map<String, Object> logging = (Map<String, Object>) config.getOrDefault("logging", new HashMap<>());
        return (String) logging.getOrDefault("level", "INFO");
    }

    /**
     * Get number of consumer threads (each runs its own KafkaConsumer instance).
     * Default: 3. Kafka broker assigns partitions within the shared consumer group.id
     * for parallel consumption. (Previous worker threads repurposed here for multi-consumer.)
     */
    public int getNumConsumerThreads() {
        return (Integer) getKafkaConfig().getOrDefault("num_consumer_threads", 3);
    }

    /**
     * Get max threads cap for dynamic partition-based threading.
     * Hardcoded to 10 per spec; if topic partitions >10, fallback to num_consumer_threads.
     */
    public int getMaxThreadsCap() {
        return 10;  // per request
    }
}