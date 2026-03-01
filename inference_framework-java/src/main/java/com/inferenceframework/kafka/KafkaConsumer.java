package com.inferenceframework.kafka;  // kafka module

// Modular imports for other packages
import com.inferenceframework.config.Config;
import com.inferenceframework.event.OutputEvent;
import com.inferenceframework.model.ModelLoader;
import com.inferenceframework.processing.LogProcessor;
import com.inferenceframework.schema.SchemaValidator;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
// For multi-consumer threads: each thread gets its own KafkaConsumer instance.
// Rebalance listener for partition assignment (Kafka broker handles across threads/group.id).
// Added: AdminClient for dynamic partition count query from brokers.
// Cleaned: removed self-import KafkaConsumer to fix duplicate symbol; removed stale ExecutorService remnant.

/**
 * Kafka consumer for real-time log messages with batch processing.
 * Uses Apache Kafka Java client (kafka-clients).
 * Subscribes to ONE topic per model (msgs conform to one schema per topic/model).
 */
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final Config config;
    private final SchemaValidator schemaValidator;
    private final LogProcessor logProcessor;
    private final ModelLoader modelLoader;
    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ObjectMapper mapper = new ObjectMapper();

    private final int batchSize;
    private final long pollTimeoutMs;
    // Model-specific (one topic/schema per model deployment)
    private final String topic;
    private final String schemaName;
    // For multi-consumer threads: single consumer thread for poll/subscribe/commit (Kafka not thread-safe).
    // (Stale processorExecutor removed to fix ExecutorService symbol error from prior remnants.)

    /**
     * Constructor builds Kafka consumer config. For multi-consumer threads.
     */
    public KafkaConsumer(Config config, SchemaValidator schemaValidator, 
                         LogProcessor logProcessor, ModelLoader modelLoader) {
        this.config = config;
        this.schemaValidator = schemaValidator;
        this.logProcessor = logProcessor;
        this.modelLoader = modelLoader;
        this.topic = modelLoader.getTopic();
        this.schemaName = modelLoader.getSchemaName();

        Map<String, Object> kafkaConfig = buildKafkaConfig();
        Properties props = new Properties();
        props.putAll(kafkaConfig);
        // Fix diamond operator inference error: fully qualify Apache KafkaConsumer (class name clash in package)
        // (self KafkaConsumer is non-generic)
        this.consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);

        this.batchSize = (Integer) config.getKafkaConfig().getOrDefault("batch_size", 100);
        // Fix lossy double to long conversion for pollTimeoutMs
        this.pollTimeoutMs = (long) ((Double) config.getKafkaConfig().getOrDefault("poll_timeout", 1.0) * 1000L);
        // For multi-consumer threads (no internal executor; threads spawned in Main).
        // Use numConsumerThreads from config.
        logger.info("Initialized KafkaConsumer instance for multi-consumer threads (partitions via brokers)");
    }

    /**
     * Query number of partitions for the topic from Kafka brokers (after connect).
     * Uses AdminClient (from kafka-clients lib). Called in Main before spawning threads.
     * Fallback to 1 on error.
     */
    public int getPartitionCount() {
        // Build AdminClient props from kafka config (bootstrap etc.)
        Map<String, Object> kafkaConfig = config.getKafkaConfig();
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getOrDefault("bootstrap_servers", "localhost:9092"));
        // Short timeout for query
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            DescribeTopicsResult result = adminClient.describeTopics(Collections.singleton(topic));
            Map<String, TopicDescription> descriptions = result.all().get(10, java.util.concurrent.TimeUnit.SECONDS);
            int partitions = descriptions.get(topic).partitions().size();
            logger.info("Queried {} partitions for topic {} from brokers", partitions, topic);
            return partitions;
        } catch (Exception e) {
            logger.warn("Failed to query partitions for topic {} (fallback to 1): {}", topic, e.getMessage());
            return 1;
        }
    }

    /**
     * Build Kafka consumer properties (Java client equivalent).
     */
    private Map<String, Object> buildKafkaConfig() {
        Map<String, Object> kc = config.getKafkaConfig();
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kc.getOrDefault("bootstrap_servers", "localhost:9092"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kc.getOrDefault("group_id", "inference-consumer"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kc.getOrDefault("auto_offset_reset", "earliest"));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kc.getOrDefault("enable_auto_commit", false));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Add more config as needed
        return props;
    }

    /**
     * Subscribe to the model's SINGLE topic.
     * For multi-consumer threads: add rebalance listener to log partition assignment
     * (Kafka broker assigns partitions to each thread's consumer in shared group.id).
     */
    public void subscribe() {
        List<String> topics = Collections.singletonList(topic);
        // Rebalance listener for visibility in multi-thread setup
        consumer.subscribe(topics, new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<org.apache.kafka.common.TopicPartition> partitions) {
                logger.info("Partitions revoked for thread {}: {}", Thread.currentThread().getName(), partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<org.apache.kafka.common.TopicPartition> partitions) {
                logger.info("Partitions assigned to thread {}: {}", Thread.currentThread().getName(), partitions);
            }
        });
        logger.info("Subscribed to model-specific topic: {} (schema={}) on thread {}", topics, schemaName, Thread.currentThread().getName());
    }

    /**
     * Fetch a batch of messages (up to batchSize or timeout).
     */
    private List<Map<String, Object>> fetchBatch() {
        List<Map<String, Object>> messages = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        Duration timeout = Duration.ofMillis((long) (pollTimeoutMs));

        while (messages.size() < batchSize && (System.currentTimeMillis() - startTime) < pollTimeoutMs) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Parse JSON value
                    Map<String, Object> value = mapper.readValue(record.value(), new TypeReference<Map<String, Object>>() {});
                    messages.add(value);
                } catch (Exception e) {
                    logger.warn("Failed to parse message: {}", e.getMessage());
                }
            }
            if (records.isEmpty()) {
                // brief yield
                try { Thread.sleep(10); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        }
        return messages;
    }

    /**
     * Validate and process messages in batch using model's schema.
     * Returns List<OutputEvent> for downstream producer.
     * (Sync version; used by async workers in multithreaded flow.)
     */
    private List<OutputEvent> processMessages(List<Map<String, Object>> messages) {
        List<Map<String, Object>> validated = new ArrayList<>();
        for (Map<String, Object> msg : messages) {
            // Use this model's schema_name
            Map<String, Object> validatedMsg = schemaValidator.validateMessage(msg, schemaName);
            if (validatedMsg != null) {
                validated.add(validatedMsg);
            } else {
                logger.debug("Dropped invalid message");
            }
        }
        if (!validated.isEmpty()) {
            return logProcessor.processBatch(validated);
        }
        return new ArrayList<>();
    }

    /**
     * (Stale async wrapper removed to fix processorExecutor symbol error from prior worker-thread remnants.
     * Processing now direct in per-thread consumer for multi-consumer setup.)
     */

    /**
     * Start consuming in loop (runs in *this* thread's KafkaConsumer instance).
     * For multi-consumer threads: each thread polls/processes independently.
     * Kafka assigns partitions across threads (same group.id).
     * outputCallback for sending events (e.g., to producer).
     */
    public void start(java.util.function.Consumer<List<OutputEvent>> outputCallback) {
        running.set(true);
        subscribe();
        logger.info("KafkaConsumer started on thread {} (for partition-based multithreading)", Thread.currentThread().getName());

        // For graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered on thread {}", Thread.currentThread().getName());
            consumer.wakeup();
        }));

        try {
            while (running.get()) {
                List<Map<String, Object>> messages = fetchBatch();
                if (!messages.isEmpty()) {
                    // Process in *this* thread (no async/worker; multi-threading at consumer level)
                    List<OutputEvent> events = processMessages(messages);
                    if (!events.isEmpty() && outputCallback != null) {
                        outputCallback.accept(events);
                    }
                    // Manual commit in this thread
                    consumer.commitSync();
                }
                // Avoid busy loop
                try { Thread.sleep(10); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
        } catch (WakeupException e) {
            // Ignore for shutdown
            logger.info("Consumer wakeup for shutdown on thread {}", Thread.currentThread().getName());
        } catch (Exception e) {
            logger.error("Consumer error on thread {}", Thread.currentThread().getName(), e);
        } finally {
            stop();
        }
    }

    /**
     * Stop *this* consumer instance (called per thread).
     */
    public void stop() {
        running.set(false);
        consumer.close();
        logger.info("KafkaConsumer stopped on thread {}", Thread.currentThread().getName());
    }
}