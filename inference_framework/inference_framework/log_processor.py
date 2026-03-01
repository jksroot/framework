from typing import List, Dict, Any
import logging
from inference_framework.event_processor import EventProcessor
from inference_framework.feature_store import FeatureStore
from inference_framework.model_loader import ModelLoader
from inference_framework.output_event import OutputEvent

logger = logging.getLogger(__name__)

class LogProcessor:
    """Processes batches of logs: to DF, fetch features, run the SINGLE loaded model."""
    
    def __init__(self, feature_store: FeatureStore, model_loader: ModelLoader):
        self.feature_store = feature_store
        self.model_loader = model_loader
        self.event_processor = EventProcessor()
        # Get the single model (enforced by ModelLoader)
        self.model = model_loader.get_model()
        if not self.model:
            raise ValueError("No model loaded by ModelLoader")
    
    def process_batch(self, validated_batch: List[Dict[str, Any]]) -> List[OutputEvent]:
        """Process validated batch: DF + features + inference via the single model.
        Returns List[OutputEvent] for Kafka producer."""
        if not validated_batch:
            return []
        
        # Convert to Polars DF
        df = self.event_processor.process_batch(validated_batch)
        if df.is_empty():
            return []
        
        # Get unique user_ids
        user_ids = df['user_id'].unique().to_list() if 'user_id' in df.columns else []
        features = self.feature_store.get_features_for_users(user_ids)
        
        # Run the single model (no loop needed)
        all_events: List[OutputEvent] = []
        try:
            model_name = self.model_loader.get_model_name()
            model_events = self.model.process(df, features)
            all_events.extend(model_events)
        except Exception as e:
            logger.error(f"Error in model {model_name}: {str(e)}")
        
        logger.info(f"Generated {len(all_events)} inference events from batch using model: {model_name}")
        return all_events
