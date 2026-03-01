import pytest
from unittest.mock import patch, mock_open
from inference_framework.config import Config

def test_config_loading():
    """Test basic config loading and properties."""
    mock_yaml = """
kafka:
  output_topic: "test_output"
  dlq_topic: "test_dlq"
models:
  login_anomaly:
    topic: "test_topic"
    schema_name: "test_schema"
"""
    with patch('builtins.open', mock_open(read_data=mock_yaml)):
        config = Config(config_path="dummy.yaml")
        assert config.output_topic == "test_output"
        assert config.dlq_topic == "test_dlq"
        model_config = config.get_model_config("login_anomaly")
        assert model_config["topic"] == "test_topic"
        assert model_config["schema_name"] == "test_schema"

def test_default_model_name():
    """Test fallback default model."""
    mock_yaml = "models:\n  login_anomaly: {}\n"
    with patch('builtins.open', mock_open(read_data=mock_yaml)):
        config = Config(config_path="dummy.yaml")
        assert config.default_model_name == "login_anomaly"
