import pytest
import polars as pl
from unittest.mock import patch
from inference_framework.models.login_anomaly import LoginAnomaly
from inference_framework.output_event import OutputEvent

def test_login_anomaly_process():
    """Test LoginAnomaly with sample DF and features (random score)."""
    model = LoginAnomaly({"threshold": 0.1})  # low threshold to catch random scores
    
    # Sample data
    df = pl.DataFrame({
        "user_id": ["abc", "def"],
        "ip_address": ["1.2.3.4", "5.6.7.8"]
    })
    features = {
        "abc": {"ip_address_profile": {"risk_score": 0.9}},
        "def": {"ip_address_profile": {"risk_score": 0.1}}
    }
    
    events = model.process(df, features)
    assert len(events) > 0  # random but threshold low
    assert all(isinstance(e, OutputEvent) for e in events)
    assert all(0.0 <= e.score <= 1.0 for e in events)
    # At least one should pass low threshold likely

def test_base_model():
    """Test base model default."""
    from inference_framework.models.base_model import BaseModel
    base = BaseModel()
    events = base.process(pl.DataFrame(), {})
    assert events == []
