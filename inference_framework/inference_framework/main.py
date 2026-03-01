import logging
import argparse
from typing import List
from inference_framework.config import Config
from inference_framework.schema_validator import SchemaValidator
from inference_framework.feature_store import FeatureStore
from inference_framework.model_loader import ModelLoader
from inference_framework.log_processor import LogProcessor
from inference_framework.kafka_consumer import KafkaConsumer
from inference_framework.kafka_producer import KafkaProducer
from inference_framework.output_event import OutputEvent

def setup_logging(log_level: str = "INFO"):
    """Setup basic logging."""
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('inference.log', mode='a')
        ]
    )

def output_events(producer: KafkaProducer, events: List[OutputEvent]):
    """Callback to send OutputEvents to common Kafka output topic (instead of printing)."""
    if events:
        producer.send_events(events)

def main():
    """Main entry point for the inference framework.
    Supports --model-name for single-model deployments (e.g., K8s per-model consumer groups).
    Models are now directly keyed by name under 'models:' in config (selects topic/schema/etc.).
    Output events sent to common Kafka topic via producer."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--model-name', type=str, default=None,
                        help='Model name to load (must match key under models: in config; e.g., login_anomaly)')
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting Real-Time Inference Framework")
    
    try:
        # Load config
        config = Config()
        
        # Initialize components
        schema_validator = SchemaValidator(config.schema_config.get('base_path', 'schemas'))
        feature_store = FeatureStore(config.features_path)
        # Pass model_name from CLI (Docker arg) for single-model per deployment
        model_loader = ModelLoader(config, model_name=args.model_name)
        log_processor = LogProcessor(feature_store, model_loader)
        
        # Kafka consumer: subscribes ONLY to this model's topic (msgs use its schema)
        # Consumer group (from config) enables K8s pod scaling per-model deployment.
        consumer = KafkaConsumer(config, schema_validator, log_processor, model_loader)
        
        # Kafka producer for common output topic (all models send here)
        producer = KafkaProducer(config)
        
        # Start consuming (pass producer to callback for sending events)
        logger.info(f"Framework initialized with model: {model_loader.get_model_name()} "
                    f"(topic={model_loader.get_topic()}, schema={model_loader.get_schema_name()}). Starting consumer...")
        # Use lambda to bind producer to callback
        consumer.start(output_callback=lambda events: output_events(producer, events))
        
    except Exception as e:
        logger.error(f"Framework startup failed: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
