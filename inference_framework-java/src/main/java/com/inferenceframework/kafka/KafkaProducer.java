package com.inferenceframework.kafka;  // kafka module

// Modular imports for other packages
import com.inferenceframework.config.Config;
import com.inferenceframework.event.OutputEvent;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Kafka producer for sending output events to common topic (used by all models).
 * Uses Apache Kafka Java client (kafka-clients).
 * Includes retry mechanism; falls back to DLQ topic on final failure.
 * Thread-safe for multithreaded consumer: sendEvents synchronized (producer not thread-safe).
 */
public class KafkaProducer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducer.class);

    private final Config config;
    private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    private final String outputTopic;
    private final String dlqTopic;
    private final int maxRetries = 3;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructor.
     */
    public KafkaProducer(Config config) {
        this.config = config;
        this.producer = new org.apache.kafka.clients.producer.KafkaProducer<>(buildProducerConfig());
        this.outputTopic = config.getOutputTopic();
        this.dlqTopic = config.getDlqTopic();
        logger.info("Initialized producer for output topic: {} (DLQ: {})", outputTopic, dlqTopic);
    }

    /**
     * Build producer properties.
     */
    private Properties buildProducerConfig() {
        Map<String, Object> kc = config.getKafkaConfig();
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kc.getOrDefault("bootstrap_servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName());
        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // etc.
        return props;
    }

    /**
     * Send list of OutputEvents to the common output topic with retries + DLQ fallback.
     * Synchronized for thread-safety (called concurrently from worker threads in multithreaded consumer).
     * KafkaProducer itself is not thread-safe.
     */
    public synchronized void sendEvents(List<OutputEvent> events) {
        for (OutputEvent event : events) {
            sendWithRetry(event);
        }
        // Flush to ensure delivery
        producer.flush();
    }

    /**
     * Attempt send with retries; move to DLQ on failure.
     */
    private void sendWithRetry(OutputEvent event) {
        Map<String, Object> eventMap = event.toMap();
        String value;
        try {
            value = mapper.writeValueAsString(eventMap);
        } catch (Exception e) {
            logger.error("Failed to serialize event", e);
            return;
        }

        boolean success = false;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                ProducerRecord<String, String> record = new ProducerRecord<>(outputTopic, event.getEventId(), value);
                // Sync send for simplicity with retry (or async with callback)
                producer.send(record).get(10, TimeUnit.SECONDS);  // wait for ack
                logger.debug("Produced event to {} (attempt {}): {}", outputTopic, attempt, event.getEventId());
                success = true;
                break;
            } catch (Exception e) {
                logger.warn("Produce attempt {}/{} failed for {}: {}", attempt, maxRetries, event.getEventId(), e.getMessage());
                if (attempt < maxRetries) {
                    long backoff = (long) (Math.pow(2, attempt - 1) * 100);  // ms
                    try { Thread.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                } else {
                    logger.error("All retries failed for event {}", event.getEventId());
                }
            }
        }

        if (!success) {
            // Fallback to DLQ
            sendToDlq(event, value);
        }
    }

    /**
     * Send failed event to DLQ topic.
     */
    private void sendToDlq(OutputEvent event, String value) {
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>(dlqTopic, event.getEventId(), value);
            producer.send(record).get(10, TimeUnit.SECONDS);
            logger.info("Event moved to DLQ {}: {}", dlqTopic, event.getEventId());
        } catch (Exception e) {
            logger.error("Failed to send to DLQ for event {}", event.getEventId(), e);
        }
    }

    /**
     * Close producer.
     */
    public void close() {
        producer.close();
    }
}