package com.inferenceframework.model;  // model module

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.inferenceframework.event.OutputEvent;  // from event module

/**
 * Abstract interface that all models must implement for pluggable inference.
 * Adapted for Java: uses List<Map> instead of Polars DataFrame for batch processing.
 */
public interface ModelInterface {

    /**
     * Process batch of messages (as List<Map>) and corresponding features.
     * Return list of OutputEvent instances (fixed format).
     *
     * @param messages List of validated message maps (from schema/JSON)
     * @param features Dict of user_id -> feature_vector
     * @return List of OutputEvent objects
     */
    List<OutputEvent> process(List<Map<String, Object>> messages, Map<String, Map<String, Object>> features);

    /**
     * Generate standard OutputEvent (models should call this).
     * Factory for consistency (auto ID/timestamp, model_id).
     */
    default OutputEvent generateOutputEvent(String userId, double score, Map<String, Object> data, String modelId) {
        if (data == null) {
            data = new HashMap<>();
        }
        if (modelId == null) {
            modelId = this.getClass().getSimpleName().toLowerCase();
        }
        return OutputEvent.create(userId, score, data, modelId);
    }
}