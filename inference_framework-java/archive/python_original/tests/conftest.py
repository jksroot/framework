import pytest
from unittest.mock import MagicMock
from inference_framework.model_loader import ModelLoader
from inference_framework.output_event import OutputEvent

@pytest.fixture
def mock_model_loader():
    """Mock ModelLoader for tests."""
    loader = MagicMock(spec=ModelLoader)
    mock_model = MagicMock()
    mock_model.process.return_value = [OutputEvent.create("test", 0.5)]
    loader.get_model.return_value = mock_model
    loader.get_model_name.return_value = "test_model"
    return loader
