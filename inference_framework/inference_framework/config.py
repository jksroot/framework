import yaml
from pathlib import Path
from typing import Dict, Any

class Config:
    """Configuration manager for the inference framework."""
    
    def __init__(self, config_path: str = "configs/config.yaml"):
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """Load YAML configuration."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    @property
    def kafka_config(self) -> Dict[str, Any]:
        """Get Kafka configuration (incl. common output_topic)."""
        return self.config.get('kafka', {})
    
    @property
    def output_topic(self) -> str:
        """Common Kafka topic for output events from ALL models."""
        return self.kafka_config.get('output_topic', 'inference_events')
    
    @property
    def dlq_topic(self) -> str:
        """Dead-letter queue topic for events that fail after retries."""
        return self.kafka_config.get('dlq_topic', 'inference_events_dlq')
    
    @property
    def schema_config(self) -> Dict[str, Any]:
        """Get schema configuration."""
        return self.config.get('schemas', {})
    
    @property
    def features_path(self) -> str:
        """Get features JSON path."""
        return self.config.get('features', {}).get('path', 'features/user_features.json')
    
    @property
    def model_configs(self) -> Dict[str, Any]:
        """Get all models (now directly keyed by model_name under 'models:'; each contains topic, schema_name, etc.)."""
        # Flattened per spec: models: { "login_anomaly": {config...}, ... }
        return self.config.get('models', {})
    
    def get_model_config(self, model_name: str) -> Dict[str, Any]:
        """Get config for a specific model (includes topic, schema_name, etc.)."""
        return self.model_configs.get(model_name, {})
    
    @property
    def default_model_name(self) -> str:
        """Fallback default model (first key in models dict)."""
        models = self.model_configs
        return list(models.keys())[0] if models else 'login_anomaly'
    
    @property
    def log_level(self) -> str:
        """Get logging level."""
        return self.config.get('logging', {}).get('level', 'INFO')
