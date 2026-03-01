package com.inferenceframework.processing;  // processing module

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages loading and retrieval of feature vectors for user_ids from JSON file.
 * Uses Jackson for JSON parsing.
 */
public class FeatureStore {
    private static final Logger logger = LoggerFactory.getLogger(FeatureStore.class);

    private final String featuresPath;
    private Map<String, Map<String, Object>> features;

    /**
     * Constructor loads features from JSON.
     */
    public FeatureStore(String featuresPath) {
        this.featuresPath = featuresPath;
        this.features = loadFeatures();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> loadFeatures() {
        // Load features from JSON file using Jackson.
        ObjectMapper mapper = new ObjectMapper();
        File file = new File(featuresPath);
        if (!file.exists()) {
            logger.warn("Features file not found: {}. Using empty store.", featuresPath);
            return new HashMap<>();
        }
        try {
            // Load as Map<String, Map> - fix incompatible type from raw Jackson Map<String, Object>
            Map<String, Object> loaded = mapper.readValue(file, Map.class);
            logger.info("Loaded features for {} users.", loaded.size());
            // Safe cast with suppression for generics (JSON deserialized structure matches)
            return (Map<String, Map<String, Object>>) (Map) loaded;
        } catch (IOException e) {
            logger.error("Failed to load features", e);
            return new HashMap<>();
        }
    }

    /**
     * Get feature vector for a user_id.
     */
    public Map<String, Object> getFeatureVector(String userId) {
        return features.getOrDefault(userId, new HashMap<>());
    }

    /**
     * Get features for multiple user_ids (batch).
     */
    public Map<String, Map<String, Object>> getFeaturesForUsers(List<String> userIds) {
        Map<String, Map<String, Object>> result = new HashMap<>();
        for (String uid : userIds) {
            result.put(uid, getFeatureVector(uid));
        }
        return result;
    }

    /**
     * Get all features (for convenience in processing).
     */
    public Map<String, Map<String, Object>> getAllFeatures() {
        return features;
    }
}