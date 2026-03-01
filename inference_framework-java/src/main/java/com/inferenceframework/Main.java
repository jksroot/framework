package com.inferenceframework;

// Modular imports for subpackages
import com.inferenceframework.config.Config;
import com.inferenceframework.event.OutputEvent;
import com.inferenceframework.kafka.KafkaConsumer;
import com.inferenceframework.kafka.KafkaProducer;
import com.inferenceframework.model.ModelLoader;
import com.inferenceframework.processing.FeatureStore;
import com.inferenceframework.processing.LogProcessor;
import com.inferenceframework.schema.SchemaValidator;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Main entry point for the Java inference framework.
 * Supports --model-name for single-model deployments (e.g., K8s).
 * Dynamic multithreaded Kafka consumers: query partitions from brokers, spawn threads = min(partitions, 10)
 * or fallback to config.num_consumer_threads if >10. Kafka assigns partitions to threads (same group.id).
 * Output events sent to common Kafka topic via producer.
 * Shutdown hooks ensure graceful close of threads/producer.
 * Refactored from Python original.
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Setup basic logging (SLF4J/Logback).
     * Level from config or env.
     */
    private static void setupLogging(String logLevel) {
        // Logback config via logback.xml or programmatic, but for simplicity assume logback.xml or default.
        // Can override level via system prop or config.
        System.setProperty("log.level", logLevel);  // if using in logback
        // Full setup would use LoggerContext, but basic for now.
    }

    /**
     * Callback to send OutputEvents to common Kafka output topic.
     */
    private static void outputEvents(KafkaProducer producer, List<OutputEvent> events) {
        if (!events.isEmpty()) {
            producer.sendEvents(events);
        }
    }

    public static void main(String[] args) {
        // CLI parser similar to Python argparse
        ArgumentParser parser = ArgumentParsers.newFor("inference-framework")
            .build()
            .defaultHelp(true)
            .description("Real-Time Inference Framework in Java");
        parser.addArgument("--model-name")
            .type(String.class)
            .setDefault((String) null)
            .help("Model name to load (must match key under models: in config; e.g., login_anomaly)");

        String modelName = null;
        try {
            Namespace ns = parser.parseArgs(args);
            modelName = ns.getString("model_name");
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            System.exit(1);
        }

        setupLogging("INFO");  // will override from config
        logger.info("Starting Real-Time Inference Framework (Java version)");

        try {
            // Load config
            Config config = new Config();

            // Setup logging from config
            setupLogging(config.getLogLevel());

            // Initialize components
            // Get base_path safely (String or default)
            Object schemaBasePath = config.getSchemaConfig().getOrDefault("base_path", "schemas");
            SchemaValidator schemaValidator = new SchemaValidator(schemaBasePath.toString());
            FeatureStore featureStore = new FeatureStore(config.getFeaturesPath());
            // Pass modelName from CLI for single-model per deployment
            ModelLoader modelLoader = new ModelLoader(config, modelName);
            LogProcessor logProcessor = new LogProcessor(featureStore, modelLoader);

            // Kafka producer for common output topic (shared across threads; synchronized for safety)
            KafkaProducer producer = new KafkaProducer(config);

            // Dynamic multi-consumer threads: query partitions from brokers using temp consumer instance
            // threads = min(partitions, config.getMaxThreadsCap()=10) or fallback to config.num_consumer_threads if >10
            // Same group.id => Kafka broker assigns partitions to threads for parallel consumption.
            // Create temp consumer just for query (lightweight, shares config)
            KafkaConsumer tempConsumerForQuery = new KafkaConsumer(config, schemaValidator, logProcessor, modelLoader);
            int partitions = tempConsumerForQuery.getPartitionCount();
            int numThreads = (partitions <= config.getMaxThreadsCap()) ? partitions : config.getNumConsumerThreads();
            ExecutorService consumerExecutor = Executors.newFixedThreadPool(numThreads);
            logger.info("Framework initialized with model: {} (topic={}, schema={}, partitions={}). Starting {} consumer threads...",
                modelLoader.getModelName(), modelLoader.getTopic(), modelLoader.getSchemaName(), partitions, numThreads);

            // Spawn consumer threads (each creates own KafkaConsumer, shares producer/callback)
            for (int i = 0; i < numThreads; i++) {
                // Each thread: fresh consumer for partition assignment
                final KafkaConsumer consumer = new KafkaConsumer(config, schemaValidator, logProcessor, modelLoader);
                consumerExecutor.submit(() -> {
                    try {
                        // Pass producer to callback; thread name for logging/rebalance
                        consumer.start(events -> outputEvents(producer, events));
                    } catch (Exception e) {
                        logger.error("Consumer thread failed", e);
                    }
                });
            }

            // Add shutdown hook for producer + executor (graceful)
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Main shutdown hook: closing producer and consumers");
                producer.close();
                consumerExecutor.shutdownNow();
            }));

            // Keep main thread alive to manage consumers (wait for executor)
            // (Shutdown hook handles interrupt; dynamic threads from partitions)
            try {
                consumerExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("Main await interrupted", e);
            }

        } catch (Exception e) {
            logger.error("Framework startup failed", e);
            System.exit(1);
        } finally {
            // Note: await keeps main running; hooks ensure cleanup for multi-consumer threads
        }
    }
}