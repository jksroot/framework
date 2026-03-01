package kafka

import (
	"context"
	"encoding/json"
	"inference_framework/internal/config"
	"inference_framework/internal/output"
	"log"
	"math"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer sends output events to the common Kafka output topic.
// Includes retry mechanism with exponential backoff; falls back to DLQ topic on final failure.
type Producer struct {
	writer     *kafkago.Writer
	dlqWriter  *kafkago.Writer
	outputTopic string
	dlqTopic    string
	maxRetries  int
}

// NewProducer creates a new Kafka producer for sending output events.
func NewProducer(cfg *config.Config) *Producer {
	outputTopic := cfg.OutputTopic()
	dlqTopic := cfg.DLQTopic()

	writer := &kafkago.Writer{
		Addr:     kafkago.TCP(cfg.Kafka.BootstrapServers),
		Topic:    outputTopic,
		Balancer: &kafkago.LeastBytes{},
	}

	dlqWriter := &kafkago.Writer{
		Addr:     kafkago.TCP(cfg.Kafka.BootstrapServers),
		Topic:    dlqTopic,
		Balancer: &kafkago.LeastBytes{},
	}

	log.Printf("Initialized producer for output topic: %s (DLQ: %s)", outputTopic, dlqTopic)

	return &Producer{
		writer:      writer,
		dlqWriter:   dlqWriter,
		outputTopic: outputTopic,
		dlqTopic:    dlqTopic,
		maxRetries:  3,
	}
}

// SendEvents sends a list of OutputEvents to the common output topic with retries + DLQ fallback.
func (p *Producer) SendEvents(ctx context.Context, events []output.Event) {
	for _, event := range events {
		p.sendWithRetry(ctx, event)
	}
}

// sendWithRetry attempts to send an event with retries; moves to DLQ on failure.
func (p *Producer) sendWithRetry(ctx context.Context, event output.Event) {
	value, err := json.Marshal(event)
	if err != nil {
		log.Printf("ERROR: Failed to marshal event %s: %v", event.EventID, err)
		return
	}

	msg := kafkago.Message{
		Value: value,
	}

	success := false
	for attempt := 1; attempt <= p.maxRetries; attempt++ {
		err := p.writer.WriteMessages(ctx, msg)
		if err == nil {
			log.Printf("Produced event to %s (attempt %d): %s", p.outputTopic, attempt, event.EventID)
			success = true
			break
		}
		log.Printf("WARNING: Produce attempt %d/%d failed for %s: %v", attempt, p.maxRetries, event.EventID, err)
		if attempt < p.maxRetries {
			backoff := time.Duration(math.Pow(2, float64(attempt-1))*100) * time.Millisecond
			time.Sleep(backoff)
		} else {
			log.Printf("ERROR: All retries failed for event %s", event.EventID)
		}
	}

	if !success {
		p.sendToDLQ(ctx, event, value)
	}
}

// sendToDLQ sends a failed event to the DLQ topic.
func (p *Producer) sendToDLQ(ctx context.Context, event output.Event, value []byte) {
	msg := kafkago.Message{
		Value: value,
	}
	err := p.dlqWriter.WriteMessages(ctx, msg)
	if err != nil {
		log.Printf("ERROR: Failed to send to DLQ %s for event %s: %v", p.dlqTopic, event.EventID, err)
	} else {
		log.Printf("Event moved to DLQ %s: %s", p.dlqTopic, event.EventID)
	}
}

// Close closes the producer writers.
func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return err
	}
	return p.dlqWriter.Close()
}
