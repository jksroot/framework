from confluent_kafka import Consumer, KafkaError, KafkaException
from typing import List, Dict, Any, Callable, Optional
import json
import time
import logging
from inference_framework.config import Config
from inference_framework.schema_validator import SchemaValidator
from inference_framework.log_processor import LogProcessor
from inference_framework.model_loader import ModelLoader
from inference_framework.output_event import OutputEvent

logger = logging.getLogger(__name__)

class KafkaConsumer:
    """Kafka consumer for real-time log messages with batch processing.
    Subscribes to ONE topic per model (msgs conform to one schema per topic/model)."""
    
    def __init__(self, config: Config, schema_validator: SchemaValidator, log_processor: LogProcessor, model_loader: ModelLoader):
        self.config = config
        self.schema_validator = schema_validator
        self.log_processor = log_processor
        self.model_loader = model_loader
        self.kafka_config = self._build_kafka_config()
        self.consumer = Consumer(self.kafka_config)
        self.running = False
        self.batch_size = config.kafka_config.get('batch_size', 100)
        self.poll_timeout = config.kafka_config.get('poll_timeout', 1.0)
        # Model-specific (one topic/schema per model deployment)
        self.topic = model_loader.get_topic()
        self.schema_name = model_loader.get_schema_name()
    
    def _build_kafka_config(self) -> Dict[str, Any]:
        """Build confluent-kafka consumer config."""
        kc = self.config.kafka_config
        return {
            'bootstrap.servers': kc.get('bootstrap_servers', 'localhost:9092'),
            'group.id': kc.get('group_id', 'inference-consumer'),
            'auto.offset.reset': kc.get('auto_offset_reset', 'earliest'),
            'enable.auto.commit': kc.get('enable_auto_commit', False),
            # Add more if needed
        }
    
    def subscribe(self):
        """Subscribe to the model's SINGLE topic (one per model deployment)."""
        # Use model-specific topic (msgs in topic conform to model's schema)
        topics = [self.topic]
        self.consumer.subscribe(topics)
        logger.info(f"Subscribed to model-specific topic: {topics} (schema={self.schema_name})")
    
    def fetch_batch(self) -> List[Dict[str, Any]]:
        """Fetch a batch of messages."""
        messages = []
        start_time = time.time()
        while len(messages) < self.batch_size and (time.time() - start_time) < self.poll_timeout:
            msg = self.consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error(f"Kafka error: {msg.error()}")
                    continue
            try:
                value = json.loads(msg.value().decode('utf-8'))
                messages.append(value)
            except Exception as e:
                logger.warning(f"Failed to parse message: {str(e)}")
        return messages
    
    def process_messages(self, messages: List[Dict[str, Any]]) -> List[OutputEvent]:
        """Validate and process messages in batch using model's schema.
        Returns List[OutputEvent] for downstream producer."""
        validated = []
        for msg in messages:
            # Use this model's schema_name (msgs in its topic conform to it)
            validated_msg = self.schema_validator.validate_message(msg, self.schema_name)
            if validated_msg:
                # Support Pydantic v1 and v2
                if hasattr(validated_msg, 'model_dump'):
                    validated.append(validated_msg.model_dump())
                else:
                    validated.append(validated_msg.dict())
            else:
                logger.debug("Dropped invalid message")
        if validated:
            return self.log_processor.process_batch(validated)
        return []
    
    def start(self, output_callback: Optional[Callable[[List[OutputEvent]], None]] = None):
        """Start consuming in loop."""
        self.running = True
        self.subscribe()
        logger.info("Kafka consumer started")
        
        try:
            while self.running:
                messages = self.fetch_batch()
                if messages:
                    events = self.process_messages(messages)
                    if events and output_callback:
                        output_callback(events)
                    # Commit offsets
                    self.consumer.commit()
                time.sleep(0.01)  # avoid busy loop
        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}")
        finally:
            self.stop()
    
    def stop(self):
        """Stop consumer."""
        self.running = False
        self.consumer.close()
        logger.info("Kafka consumer stopped")
