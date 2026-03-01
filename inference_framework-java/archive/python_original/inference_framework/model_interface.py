from abc import ABC, abstractmethod
from typing import Dict, Any, List
import polars as pl
from inference_framework.output_event import OutputEvent

class ModelInterface(ABC):
    """Abstract interface that all models must implement for pluggable inference."""
    
    @abstractmethod
    def process(self, df: pl.DataFrame, features: Dict[str, Dict[str, Any]]) -> List[OutputEvent]:
        """
        Process the Polars DataFrame of messages and corresponding features.
        Return list of OutputEvent instances (fixed format).
        
        Args:
            df: Polars DataFrame with validated messages (columns from schema)
            features: Dict of user_id -> feature_vector
        
        Returns:
            List of OutputEvent objects
        """
        pass
    
    def generate_output_event(self, user_id: str, score: float, data: Dict[str, Any] = None, model_id: str = None) -> OutputEvent:
        """Generate standard OutputEvent (models should call this or use factory)."""
        if data is None:
            data = {}
        if model_id is None:
            model_id = self.__class__.__name__.lower()
        # Use factory for consistency (auto ID/timestamp)
        return OutputEvent.create(user_id=user_id, score=score, data=data, model_id=model_id)
