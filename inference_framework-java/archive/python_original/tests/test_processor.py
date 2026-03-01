import pytest
import polars as pl
from unittest.mock import MagicMock, patch, mock_open
from inference_framework.log_processor import LogProcessor
from inference_framework.event_processor import EventProcessor
from inference_framework.feature_store import FeatureStore

def test_event_processor():
    """Test batch to DF conversion."""
    processor = EventProcessor()
    msgs = [{"user_id": "abc", "log_type": "login"}]
    df = processor.process_batch(msgs)
    assert not df.is_empty()
    assert "user_id" in df.columns

def test_feature_store():
    """Test feature loading with mock."""
    mock_features = '{"abc": {"version": "2024"}}'
    with patch('builtins.open', mock_open(read_data=mock_features)):
        with patch('json.load', return_value={"abc": {"version": "2024"}}):
            store = FeatureStore("features/user_features.json")
            feats = store.get_feature_vector("abc")
            assert feats is not None
            assert "version" in feats

@pytest.mark.usefixtures("mock_model_loader")
def test_log_processor(mock_model_loader):
    """Test LogProcessor with mocked model."""
    # Simple test
    processor = LogProcessor(MagicMock(), mock_model_loader)
    # Would need full mock for process, but basic init ok
    assert processor.model is not None
