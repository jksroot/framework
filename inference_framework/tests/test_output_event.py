import pytest
from inference_framework.output_event import OutputEvent

def test_output_event_creation():
    """Test OutputEvent factory and fields."""
    event = OutputEvent.create(
        user_id="test_user",
        score=0.75,
        data={"reason": "test"},
        model_id="test_model"
    )
    assert event.user_id == "test_user"
    assert 0.0 <= event.score <= 1.0
    assert event.model_id == "test_model"
    assert "event_id" in event.to_dict()
    assert isinstance(event.timestamp, str)

def test_output_event_to_dict():
    """Test serialization."""
    event = OutputEvent.create("user", 0.5)
    d = event.to_dict()
    assert isinstance(d, dict)
    assert d["score"] == 0.5
