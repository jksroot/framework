// Package kafka provides Kafka consumer and producer implementations using segmentio/kafka-go.
package kafka

import (
	"context"
	"encoding/json"
	"inference_framework/internal/config"
	"inference_framework/internal/features"
	"inference_framework/internal/models"
	"inference_framework/internal/processor"
	"inference_framework/internal/schema"
	"log"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// ConsumerInstance represents a single Kafka consumer instance that processes
// messages independently. Multiple instances share the same consumer group,
// and Kafka handles partition assignment.
type ConsumerInstance struct {
	id              int
	reader          *kafkago.Reader
	schemaValidator *schema.Validator
	logProcessor    *processor.LogProcessor
	schemaName      string
	batchSize       int
	pollTimeout     time.Duration
	producer        *Producer
	wg              *sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

// ConsumerManager manages multiple Kafka consumer instances within the same consumer group.
type ConsumerManager struct {
	cfg             *config.Config
	schemaValidator *schema.Validator
	featureStore    *features.Store
	modelLoader     *models.Loader
	producer        *Producer
	instances       []*ConsumerInstance
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewConsumerManager creates a manager that will spawn multiple Kafka consumer instances.
// Each instance is an independent consumer in the same consumer group.
func NewConsumerManager(
	cfg *config.Config,
	schemaValidator *schema.Validator,
	featureStore *features.Store,
	modelLoader *models.Loader,
	producer *Producer,
) *ConsumerManager {
	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumerManager{
		cfg:             cfg,
		schemaValidator: schemaValidator,
		featureStore:    featureStore,
		modelLoader:     modelLoader,
		producer:        producer,
		instances:       make([]*ConsumerInstance, 0),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// getPartitionCount fetches the number of partitions for the topic from Kafka.
func (cm *ConsumerManager) getPartitionCount() (int, error) {
	topic := cm.modelLoader.GetTopic()
	
	// Create a connection to Kafka to fetch metadata
	conn, err := kafkago.Dial("tcp", cm.cfg.Kafka.BootstrapServers)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	// Get partitions for the topic
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return 0, err
	}

	return len(partitions), nil
}

// determineConsumerCount calculates the number of consumers to spawn.
// If UsePartitionCount is true, uses min(partition count, MaxInstances).
// If partitions > MaxInstances, falls back to configured NumInstances.
func (cm *ConsumerManager) determineConsumerCount() int {
	cfg := cm.cfg.Consumers

	// If not using partition count, use configured value
	if !cfg.UsePartitionCount {
		if cfg.NumInstances <= 0 {
			return 1
		}
		return cfg.NumInstances
	}

	// Fetch partition count from Kafka
	partitionCount, err := cm.getPartitionCount()
	if err != nil {
		log.Printf("WARNING: Failed to get partition count: %v. Using configured NumInstances.", err)
		if cfg.NumInstances <= 0 {
			return 1
		}
		return cfg.NumInstances
	}

	log.Printf("Topic '%s' has %d partitions", cm.modelLoader.GetTopic(), partitionCount)

	// If partitions > MaxInstances, fall back to configured NumInstances
	if partitionCount > cfg.MaxInstances {
		log.Printf("Partition count (%d) exceeds MaxInstances (%d). Falling back to configured NumInstances (%d).",
			partitionCount, cfg.MaxInstances, cfg.NumInstances)
		if cfg.NumInstances <= 0 {
			return 1
		}
		return cfg.NumInstances
	}

	// Use partition count (one consumer per partition for optimal parallelism)
	log.Printf("Using partition count (%d) as number of consumers", partitionCount)
	return partitionCount
}

// Start spawns multiple consumer instances. Each instance is an independent
// Kafka consumer in the same consumer group.
func (cm *ConsumerManager) Start() {
	numConsumers := cm.determineConsumerCount()
	if numConsumers <= 0 {
		numConsumers = 1
	}

	log.Printf("Starting %d Kafka consumer instances in group '%s' for topic '%s'",
		numConsumers, cm.cfg.Kafka.GroupID, cm.modelLoader.GetTopic())

	// Create a log processor for each consumer instance
	// Each instance gets its own log processor (which contains the model)
	// This ensures thread-safe model processing
	for i := 0; i < numConsumers; i++ {
		instance := cm.createInstance(i)
		cm.instances = append(cm.instances, instance)
		
		cm.wg.Add(1)
		go instance.run()
	}
}

// createInstance creates a new consumer instance with its own Kafka reader.
func (cm *ConsumerManager) createInstance(id int) *ConsumerInstance {
	topic := cm.modelLoader.GetTopic()
	schemaName := cm.modelLoader.GetSchemaName()

	pollTimeout := time.Duration(cm.cfg.Kafka.PollTimeout * float64(time.Second))
	if pollTimeout == 0 {
		pollTimeout = 1 * time.Second
	}

	batchSize := cm.cfg.Kafka.BatchSize
	if batchSize == 0 {
		batchSize = 100
	}

	// Each instance has its own Kafka reader with the same group ID
	// Kafka will assign different partitions to each reader
	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        []string{cm.cfg.Kafka.BootstrapServers},
		Topic:          topic,
		GroupID:        cm.cfg.Kafka.GroupID,
		StartOffset:    kafkago.FirstOffset,
		CommitInterval: 0, // manual commit
		MaxWait:        pollTimeout,
		// Each reader in the same group gets a unique MemberID via the default behavior
	})

	// Create a log processor for this instance
	// Note: We create a new loader that wraps the same model for thread safety
	// Since the model itself is stateless (only uses config), this is safe
	logProcessor, err := processor.NewLogProcessor(cm.featureStore, cm.modelLoader)
	if err != nil {
		log.Fatalf("Failed to create log processor for consumer %d: %v", id, err)
	}

	instanceCtx, instanceCancel := context.WithCancel(cm.ctx)

	return &ConsumerInstance{
		id:              id,
		reader:          reader,
		schemaValidator: cm.schemaValidator,
		logProcessor:    logProcessor,
		schemaName:      schemaName,
		batchSize:       batchSize,
		pollTimeout:     pollTimeout,
		producer:        cm.producer,
		wg:              &cm.wg,
		ctx:             instanceCtx,
		cancel:          instanceCancel,
	}
}

// run is the main loop for a consumer instance.
// Each instance independently reads from Kafka, processes messages, and produces results.
func (ci *ConsumerInstance) run() {
	defer ci.wg.Done()
	log.Printf("Consumer instance %d started", ci.id)

	for {
		select {
		case <-ci.ctx.Done():
			log.Printf("Consumer instance %d shutting down", ci.id)
			ci.close()
			return
		default:
			ci.processBatch()
		}
	}
}

// processBatch reads and processes a batch of messages.
func (ci *ConsumerInstance) processBatch() {
	messages := ci.fetchBatch()
	if len(messages) == 0 {
		time.Sleep(10 * time.Millisecond)
		return
	}

	log.Printf("Consumer %d: fetched %d messages", ci.id, len(messages))

	// Validate messages
	var validated []map[string]interface{}
	for _, msg := range messages {
		validatedMsg, err := ci.schemaValidator.ValidateMessage(msg, ci.schemaName)
		if err != nil {
			log.Printf("Consumer %d: dropped invalid message: %v", ci.id, err)
			continue
		}
		validated = append(validated, validatedMsg)
	}

	if len(validated) == 0 {
		return
	}

	// Process through model
	events := ci.logProcessor.ProcessBatch(validated)
	
	if len(events) > 0 {
		log.Printf("Consumer %d: produced %d events", ci.id, len(events))
		// Send events directly to Kafka
		ci.producer.SendEvents(ci.ctx, events)
	}
}

// fetchBatch reads a batch of messages from Kafka.
func (ci *ConsumerInstance) fetchBatch() []map[string]interface{} {
	var messages []map[string]interface{}
	deadline := time.Now().Add(ci.pollTimeout)

	for len(messages) < ci.batchSize && time.Now().Before(deadline) {
		readCtx, cancel := context.WithDeadline(ci.ctx, deadline)
		msg, err := ci.reader.ReadMessage(readCtx)
		cancel()

		if err != nil {
			if ci.ctx.Err() != nil {
				break // context cancelled
			}
			// Timeout or other error
			break
		}

		var value map[string]interface{}
		if err := json.Unmarshal(msg.Value, &value); err != nil {
			log.Printf("Consumer %d: failed to parse message: %v", ci.id, err)
			continue
		}
		messages = append(messages, value)
	}

	return messages
}

// close cleans up the consumer instance.
func (ci *ConsumerInstance) close() {
	if err := ci.reader.Close(); err != nil {
		log.Printf("Consumer %d: error closing reader: %v", ci.id, err)
	}
	log.Printf("Consumer instance %d stopped", ci.id)
}

// Stop signals all consumer instances to stop and waits for them to finish.
func (cm *ConsumerManager) Stop() {
	log.Println("Stopping all consumer instances...")
	cm.cancel()
	cm.wg.Wait()
	log.Println("All consumer instances stopped")
}
