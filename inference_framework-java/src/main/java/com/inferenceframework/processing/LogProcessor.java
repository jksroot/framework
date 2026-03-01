package com.inferenceframework.processing;  // processing module

import com.inferenceframework.event.OutputEvent;  // from event module
import com.inferenceframework.model.ModelInterface;
import com.inferenceframework.model.ModelLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Processes batches of logs: fetch features, run the SINGLE loaded model.
 * Adapted: uses EventProcessor to List<Map> (no Polars), then model inference.
 * Thread-safe: stateless per-batch, shared deps (FeatureStore, models) are read-only/immutable after init.
 */
public class LogProcessor {
    private static final Logger logger = LoggerFactory.getLogger(LogProcessor.class);

    private final FeatureStore featureStore;
    private final ModelLoader modelLoader;
    private final EventProcessor eventProcessor;
    // Get the single model (enforced by ModelLoader)
    private final ModelInterface model;

    /**
     * Constructor.
     */
    public LogProcessor(FeatureStore featureStore, ModelLoader modelLoader) {
        this.featureStore = featureStore;
        this.modelLoader = modelLoader;
        this.eventProcessor = new EventProcessor();
        // Get the single model
        this.model = modelLoader.getModel();
        if (this.model == null) {
            throw new IllegalArgumentException("No model loaded by ModelLoader");
        }
    }

    /**
     * Process validated batch: to processable format + features + inference via the single model.
     * Returns List<OutputEvent> for Kafka producer.
     */
    public List<OutputEvent> processBatch(List<Map<String, Object>> validatedBatch) {
        if (validatedBatch == null || validatedBatch.isEmpty()) {
            return new ArrayList<>();
        }

        // Convert/process batch (returns List<Map> in Java)
        List<Map<String, Object>> processed = eventProcessor.processBatch(validatedBatch);
        if (processed.isEmpty()) {
            return new ArrayList<>();
        }

        // Get unique user_ids using helper
        Set<String> userIds = eventProcessor.extractUserIds(processed);
        Map<String, Map<String, Object>> features = featureStore.getFeaturesForUsers(new ArrayList<>(userIds));

        // Run the single model (no loop; model handles batch)
        List<OutputEvent> allEvents = new ArrayList<>();
        try {
            String modelName = modelLoader.getModelName();
            List<OutputEvent> modelEvents = model.process(processed, features);
            allEvents.addAll(modelEvents);
            logger.info("Generated {} inference events from batch using model: {}", allEvents.size(), modelName);
        } catch (Exception e) {
            String modelName = modelLoader.getModelName();
            logger.error("Error in model {}", modelName, e);
        }

        return allEvents;
    }
}