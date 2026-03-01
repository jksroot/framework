// Package processor handles concurrent batch processing using a worker pool pattern.
package processor

import (
	"context"
	"inference_framework/internal/output"
	"inference_framework/internal/schema"
	"log"
	"sync"
)

// WorkItem represents a batch of messages to be processed by a worker.
type WorkItem struct {
	BatchID  int
	Messages []map[string]interface{}
}

// ResultItem represents the processing result from a worker.
type ResultItem struct {
	BatchID int
	Events  []output.Event
	Error   error
}

// ConcurrentProcessor manages a pool of workers for parallel message processing.
type ConcurrentProcessor struct {
	schemaValidator *schema.Validator
	logProcessor    *LogProcessor
	schemaName      string

	numWorkers      int
	workQueue       chan WorkItem
	resultQueue     chan ResultItem

	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
}

// NewConcurrentProcessor creates a new concurrent processor with the specified number of workers.
func NewConcurrentProcessor(
	numWorkers int,
	workQueueSize int,
	resultQueueSize int,
	schemaValidator *schema.Validator,
	logProcessor *LogProcessor,
	schemaName string,
) *ConcurrentProcessor {
	ctx, cancel := context.WithCancel(context.Background())

	return &ConcurrentProcessor{
		schemaValidator: schemaValidator,
		logProcessor:    logProcessor,
		schemaName:      schemaName,
		numWorkers:      numWorkers,
		workQueue:       make(chan WorkItem, workQueueSize),
		resultQueue:     make(chan ResultItem, resultQueueSize),
		ctx:             ctx,
		cancel:          cancel,
	}
}

// Start initializes and starts the worker pool.
func (cp *ConcurrentProcessor) Start() {
	log.Printf("Starting concurrent processor with %d workers", cp.numWorkers)
	for i := 0; i < cp.numWorkers; i++ {
		cp.wg.Add(1)
		go cp.worker(i)
	}
}

// Stop signals all workers to stop and waits for them to finish.
func (cp *ConcurrentProcessor) Stop() {
	log.Println("Stopping concurrent processor...")
	cp.cancel()
	close(cp.workQueue)
	cp.wg.Wait()
	close(cp.resultQueue)
	log.Println("Concurrent processor stopped")
}

// Submit sends a work item to the worker pool. Returns false if the context is cancelled.
func (cp *ConcurrentProcessor) Submit(item WorkItem) bool {
	select {
	case <-cp.ctx.Done():
		return false
	case cp.workQueue <- item:
		return true
	}
}

// Results returns the result channel for reading processed events.
func (cp *ConcurrentProcessor) Results() <-chan ResultItem {
	return cp.resultQueue
}

// worker is the goroutine that processes work items.
func (cp *ConcurrentProcessor) worker(id int) {
	defer cp.wg.Done()
	log.Printf("Worker %d started", id)

	for {
		select {
		case <-cp.ctx.Done():
			log.Printf("Worker %d shutting down (context cancelled)", id)
			return
		case item, ok := <-cp.workQueue:
			if !ok {
				log.Printf("Worker %d shutting down (work queue closed)", id)
				return
			}
			events := cp.processBatch(item)
			result := ResultItem{
				BatchID: item.BatchID,
				Events:  events,
			}
			select {
			case <-cp.ctx.Done():
				return
			case cp.resultQueue <- result:
			}
		}
	}
}

// processBatch validates and processes a batch of messages.
func (cp *ConcurrentProcessor) processBatch(item WorkItem) []output.Event {
	// Validate messages
	var validated []map[string]interface{}
	for _, msg := range item.Messages {
		validatedMsg, err := cp.schemaValidator.ValidateMessage(msg, cp.schemaName)
		if err != nil {
			log.Printf("DEBUG: Dropped invalid message: %v", err)
			continue
		}
		validated = append(validated, validatedMsg)
	}

	if len(validated) == 0 {
		return nil
	}

	// Process through log processor (which runs the model)
	return cp.logProcessor.ProcessBatch(validated)
}