import pytest
from unittest.mock import patch, MagicMock
from inference_framework.model_loader import ModelLoader
from inference_framework.config import Config

def test_model_loader_single_model():
    """Test loading single model with mocks."""
    mock_config = MagicMock(spec=Config)
    mock_config.default_model_name = "login_anomaly"
    mock_config.get_model_config.return_value = {"threshold": 0.5}
    
    with patch('importlib.import_module') as mock_import:
        with patch('builtins.getattr') as mock_getattr:
            mock_cls = MagicMock()
            mock_getattr.return_value = mock_cls
            loader = ModelLoader(mock_config, model_name="login_anomaly")
            assert loader.model is not None
            assert loader.get_model_name() == "login_anomaly"
            assert loader.get_topic() == "app_logs"  # fallback in code