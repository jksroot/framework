package com.inferenceframework.event;  // event module

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Standard output event POJO for all models.
 * Serialized to JSON for Kafka.
 */
public class OutputEvent {
    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("model_id")
    private String modelId;

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("score")
    private double score;

    @JsonProperty("data")
    private Map<String, Object> data;

    /**
     * Default constructor for Jackson.
     */
    public OutputEvent() {
        this.data = new HashMap<>();
    }

    /**
     * Full constructor.
     */
    public OutputEvent(String eventId, String userId, String modelId, String timestamp, double score, Map<String, Object> data) {
        this.eventId = eventId;
        this.userId = userId;
        this.modelId = modelId;
        this.timestamp = timestamp;
        this.score = score;
        this.data = data != null ? data : new HashMap<>();
    }

    /**
     * Factory to create event with auto-generated ID/timestamp.
     */
    public static OutputEvent create(String userId, double score, Map<String, Object> data, String modelId) {
        if (data == null) {
            data = new HashMap<>();
        }
        if (modelId == null) {
            modelId = "unknown_model";
        }
        return new OutputEvent(
            UUID.randomUUID().toString(),
            userId,
            modelId,
            Instant.now().toString(),
            score,
            data
        );
    }

    // Getters and Setters
    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getModelId() { return modelId; }
    public void setModelId(String modelId) { this.modelId = modelId; }

    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }

    public double getScore() { return score; }
    public void setScore(double score) { this.score = score; }

    public Map<String, Object> getData() { return data; }
    public void setData(Map<String, Object> data) { this.data = data; }

    /**
     * For serialization compatibility.
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("event_id", eventId);
        map.put("user_id", userId);
        map.put("model_id", modelId);
        map.put("timestamp", timestamp);
        map.put("score", score);
        map.put("data", data);
        return map;
    }
}