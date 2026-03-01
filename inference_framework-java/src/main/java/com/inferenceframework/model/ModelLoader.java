package com.inferenceframework.model;  // model module

import com.inferenceframework.config.Config;
import com.inferenceframework.model.models.BaseModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Loads exactly ONE model by name (per-deployment, e.g., K8s pod set in one consumer group).
 * Stores a single ModelInterface instance.
 * Uses reflection for dynamic loading similar to Python importlib.
 */
public class ModelLoader {
    private static final Logger logger = LoggerFactory.getLogger(ModelLoader.class);

    private final Config config;
    private final String modelName;
    private ModelInterface model;
    // Model-specific: topic (for subscribe) and schema (for validation)
    private String topic = "";
    private String schemaName = "";

    /**
     * Constructor. modelName from CLI/Docker arg or fallback.
     */
    public ModelLoader(Config config, String modelName) {
        this.config = config;
        // Use provided or fallback to first in config
        this.modelName = (modelName != null && !modelName.isEmpty()) ? modelName : config.getDefaultModelName();
        loadModel();
    }

    /**
     * Dynamically load the single specified model + its topic/schema from config using reflection.
     * Updated for modular packages: models now under com.inferenceframework.model.models.
     */
    @SuppressWarnings("unchecked")
    private void loadModel() {
        Map<String, Object> modelConfig = config.getModelConfig(modelName);
        try {
            // Assume models in com.inferenceframework.model.models.<ClassName> (post-modularization)
            // e.g., for "login_anomaly" -> LoginAnomaly
            // Manual camelCase conversion to avoid stream reduce edge cases
            String[] parts = modelName.split("_");
            StringBuilder classNameBuilder = new StringBuilder("com.inferenceframework.model.models.");
            for (String part : parts) {
                if (!part.isEmpty()) {
                    classNameBuilder.append(Character.toUpperCase(part.charAt(0)))
                                    .append(part.substring(1).toLowerCase());
                }
            }
            String className = classNameBuilder.toString();
            Class<? extends BaseModel> modelCls = (Class<? extends BaseModel>) Class.forName(className);
            // Instantiate with model config
            java.lang.reflect.Constructor<? extends BaseModel> ctor = modelCls.getConstructor(Map.class);
            this.model = ctor.newInstance(modelConfig);

            // Extract model-specific settings
            this.topic = (String) modelConfig.getOrDefault("topic", "app_logs");
            this.schemaName = (String) modelConfig.getOrDefault("schema_name", "app_log_type_login");
            logger.info("Loaded single model for this deployment: {} (topic={}, schema={})", modelName, topic, schemaName);
        } catch (Exception e) {
            logger.error("Failed to load model {}", modelName, e);
            throw new RuntimeException("Model loading failed for: " + modelName, e);
        }
    }

    public ModelInterface getModel() {
        return model;
    }

    public String getModelName() {
        return modelName;
    }

    public String getTopic() {
        return topic;
    }

    public String getSchemaName() {
        return schemaName;
    }
}