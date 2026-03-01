package com.inferenceframework.model.models;  // model sub-module

import com.inferenceframework.event.OutputEvent;
import com.inferenceframework.model.models.BaseModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Example model for detecting login anomalies based on features.
 * Adapted for Java: processes List<Map> messages (no Polars).
 * Assigns random score (0-1) to each generated event as specified.
 */
public class LoginAnomaly extends BaseModel {
    private static final Logger logger = LoggerFactory.getLogger(LoginAnomaly.class);

    private final double threshold;
    private final Random random;

    /**
     * Constructor.
     */
    public LoginAnomaly(Map<String, Object> config) {
        super(config);
        this.threshold = config != null && config.containsKey("threshold") 
            ? ((Number) config.get("threshold")).doubleValue() : 0.5;
        this.random = new Random();
    }

    /**
     * Simple anomaly detection: high risk IP or unusual country.
     * Assigns random score (0-1) to each generated event as specified.
     * Processes messages as List<Map> for batch.
     */
    @Override
    public List<OutputEvent> process(List<Map<String, Object>> messages, Map<String, Map<String, Object>> features) {
        List<OutputEvent> events = new ArrayList<>();
        // Simple loop over messages (Polars alternative using Java collections)

        for (Map<String, Object> row : messages) {
            String userId = (String) row.get("user_id");
            if (userId == null || !features.containsKey(userId)) {
                continue;
            }
            Map<String, Object> feat = features.get(userId);

            // Simple scoring logic (base)
            @SuppressWarnings("unchecked")
            Map<String, Object> ipProfile = (Map<String, Object>) feat.getOrDefault("ip_address_profile", new HashMap<>());
            double ipRisk = ((Number) ipProfile.getOrDefault("risk_score", 0.0)).doubleValue();

            @SuppressWarnings("unchecked")
            Map<String, Object> countryProfile = (Map<String, Object>) feat.getOrDefault("country_code_profile", new HashMap<>());
            int countryFreq = ((Number) countryProfile.getOrDefault("travel_freq", 0)).intValue();

            double baseScore = Math.min(1.0, (ipRisk + countryFreq * 0.1) * 1.5);

            // Assign random score (0-1) to event as per requirements
            double score = random.nextDouble();  // 0.0 to 1.0

            Map<String, Object> data = new HashMap<>();
            data.put("ip_address", row.get("ip_address"));
            data.put("country_code", row.get("country_code"));
            data.put("user_agent", row.get("user_agent"));
            data.put("reason", ipRisk > 0.5 ? "high_risk_ip" : "unusual_country");
            data.put("base_score", baseScore);  // for reference

            // Generate via interface (uses OutputEvent factory)
            if (score >= threshold) {  // still filter on random score for demo
                OutputEvent event = generateOutputEvent(userId, score, data, modelId);
                events.add(event);
                logger.info("Detected anomaly for user {} with random score {}", userId, score);
            }
        }

        return events;
    }
}