from confluent_kafka import Producer
from typing import List, Dict, Any
import json
import logging
import time
from inference_framework.config import Config
from inference_framework.output_event import OutputEvent

logger = logging.getLogger(__name__)

class KafkaProducer:
    """Kafka producer for sending output events to common topic (used by all models).
    Includes retry mechanism; falls back to DLQ topic on final failure."""
    
    def __init__(self, config: Config):
        self.config = config
        self.producer = Producer(self._build_producer_config())
        self.output_topic = config.output_topic
        self.dlq_topic = config.dlq_topic
        self.max_retries = 3  # configurable if needed
        logger.info(f"Initialized producer for output topic: {self.output_topic} (DLQ: {self.dlq_topic})")
    
    def _build_producer_config(self) -> Dict[str, Any]:
        """Build confluent-kafka producer config."""
        kc = self.config.kafka_config
        return {
            'bootstrap.servers': kc.get('bootstrap_servers', 'localhost:9092'),
            # Add delivery callbacks, etc. for prod
        }
    
    def send_events(self, events: List[OutputEvent]):
        """Send list of OutputEvents to the common output topic with retries + DLQ fallback."""
        for event in events:
            self._send_with_retry(event)
        # Flush to ensure delivery
        self.producer.flush()
    
    def _send_with_retry(self, event: OutputEvent):
        """Attempt send with retries; move to DLQ on failure."""
        event_dict = event.to_dict()
        value = json.dumps(event_dict).encode('utf-8')
        success = False
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.producer.produce(
                    topic=self.output_topic,
                    value=value,
                    callback=self._delivery_callback
                )
                logger.debug(f"Produced event to {self.output_topic} (attempt {attempt}): {event.event_id}")
                success = True
                break
            except Exception as e:
                logger.warning(f"Produce attempt {attempt}/{self.max_retries} failed for {event.event_id}: {str(e)}")
                if attempt < self.max_retries:
                    backoff = (2 ** (attempt - 1)) * 0.1  # simple exponential backoff
                    time.sleep(backoff)
                else:
                    logger.error(f"All retries failed for event {event.event_id}")
        
        if not success:
            # Fallback to DLQ
            self._send_to_dlq(event, value)
    
    def _send_to_dlq(self, event: OutputEvent, value: bytes):
        """Send failed event to DLQ topic."""
        try:
            self.producer.produce(
                topic=self.dlq_topic,
                value=value,
                callback=self._delivery_callback
            )
            logger.info(f"Event moved to DLQ {self.dlq_topic}: {event.event_id}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ even: {str(e)} for event {event.event_id}")
    
    def _delivery_callback(self, err, msg):
        """Callback for delivery reports."""
        if err is not None:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")