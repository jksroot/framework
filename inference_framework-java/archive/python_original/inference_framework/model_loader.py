from typing import Any, Type, Optional
import importlib
import logging
from inference_framework.model_interface import ModelInterface
from inference_framework.config import Config

logger = logging.getLogger(__name__)

class ModelLoader:
    """Loads exactly ONE model by name (per-deployment, e.g., K8s pod set in one consumer group).
    Stores a single ModelInterface instance (not a dict) to enforce single-model usage."""
    
    def __init__(self, config: Config, model_name: Optional[str] = None):
        self.config = config
        # Use provided model_name (e.g., Docker/CLI arg) or fallback to first model in config
        # (models now directly keyed by name under 'models:')
        self.model_name = model_name or config.default_model_name
        self.model: Optional[ModelInterface] = None
        # Model-specific: topic (for subscribe) and schema (for validation)
        self.topic: str = ""
        self.schema_name: str = ""
        self._load_model()
    
    def _load_model(self):
        """Dynamically load the single specified model + its topic/schema from config."""
        model_name = self.model_name
        # Use new getter for full per-model config (incl. topic/schema)
        model_config = self.config.get_model_config(model_name)
        try:
            # Assume models are in inference_framework.models.<model_name>
            module_name = f"inference_framework.models.{model_name.lower()}"
            module = importlib.import_module(module_name)
            # Get class: e.g., LoginAnomaly from login_anomaly
            class_name = ''.join(word.capitalize() for word in model_name.split('_'))
            model_cls = getattr(module, class_name)
            self.model = model_cls(model_config)
            # Extract model-specific settings
            self.topic = model_config.get('topic', 'app_logs')  # fallback
            self.schema_name = model_config.get('schema_name', 'app_log_type_login')
            logger.info(f"Loaded single model for this deployment: {model_name} (topic={self.topic}, schema={self.schema_name})")
        except Exception as e:
            logger.error(f"Failed to load model {model_name}: {str(e)}")
            raise  # Fail fast for deployment
    
    def get_model(self) -> Optional[ModelInterface]:
        """Return the single loaded model interface."""
        return self.model
    
    def get_model_name(self) -> str:
        """Return the deployed model name."""
        return self.model_name
    
    def get_topic(self) -> str:
        """Return the topic this model subscribes to (one per model)."""
        return self.topic
    
    def get_schema_name(self) -> str:
        """Return the schema for msgs in this model's topic."""
        return self.schema_name


