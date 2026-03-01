package com.inferenceframework.processing;  // processing module

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts batch of validated messages to processable format.
 * In Java refactor: simply returns List<Map> (Polars DataFrame replaced by collection for simplicity and no extra deps).
 * Keeps interface for compatibility in processing pipeline.
 */
public class EventProcessor {
    private static final Logger logger = LoggerFactory.getLogger(EventProcessor.class);

    /**
     * Process batch: in Java, pass through List<Map<String, Object>>.
     * (No heavy DataFrame lib like Polars; use streams/collections instead.)
     */
    public List<Map<String, Object>> processBatch(List<Map<String, Object>> validatedMessages) {
        if (validatedMessages == null || validatedMessages.isEmpty()) {
            return new java.util.ArrayList<>();
        }
        try {
            logger.debug("Processed batch with {} messages.", validatedMessages.size());
            // Could add filtering, field extraction here using Java streams if needed
            return validatedMessages;
        } catch (Exception e) {
            logger.error("Failed to process batch", e);
            return new java.util.ArrayList<>();
        }
    }

    /**
     * Extract unique user_ids from messages for feature lookup.
     */
    public Set<String> extractUserIds(List<Map<String, Object>> messages) {
        return messages.stream()
            .map(msg -> (String) msg.get("user_id"))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }
}