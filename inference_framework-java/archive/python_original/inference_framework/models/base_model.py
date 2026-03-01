"""Base implementation of ModelInterface."""
from inference_framework.model_interface import ModelInterface
from inference_framework.output_event import OutputEvent
from typing import Dict, Any, List
import polars as pl
import logging

logger = logging.getLogger(__name__)

class BaseModel(ModelInterface):
    """Base model with common utilities."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or {}
        self.model_id = self.__class__.__name__.lower()
    
    def process(self, df: pl.DataFrame, features: Dict[str, Dict[str, Any]]) -> List[OutputEvent]:
        """Default process - should be overridden."""
        logger.warning(f"BaseModel process called - override in subclass")
        return []
