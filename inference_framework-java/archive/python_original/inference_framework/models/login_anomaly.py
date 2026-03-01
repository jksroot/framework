from inference_framework.models.base_model import BaseModel
from inference_framework.output_event import OutputEvent
from typing import Dict, Any, List
import polars as pl
import logging
import random  # For random score assignment as specified

logger = logging.getLogger(__name__)

class LoginAnomaly(BaseModel):
    """Example model for detecting login anomalies based on features."""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self.threshold = config.get('threshold', 0.5) if config else 0.5
    
    def process(self, df: pl.DataFrame, features: Dict[str, Dict[str, Any]]) -> List[OutputEvent]:
        """
        Simple anomaly detection: high risk IP or unusual country.
        Assigns random score (0-1) to each generated event as specified.
        """
        events = []
        # Convert to dict for row processing (for simplicity)
        rows = df.to_dicts()
        
        for row in rows:
            user_id = row.get('user_id')
            if not user_id or user_id not in features:
                continue
            feat = features[user_id]
            
            # Simple scoring logic (base)
            ip_risk = feat.get('ip_address_profile', {}).get('risk_score', 0.0)
            country_freq = feat.get('country_code_profile', {}).get('travel_freq', 0)
            base_score = min(1.0, (ip_risk + country_freq * 0.1) * 1.5)
            
            # Assign random score (0-1) to event as per requirements
            score = random.uniform(0.0, 1.0)
            
            data = {
                "ip_address": row.get('ip_address'),
                "country_code": row.get('country_code'),
                "user_agent": row.get('user_agent'),
                "reason": "high_risk_ip" if ip_risk > 0.5 else "unusual_country",
                "base_score": base_score  # for reference
            }
            
            # Generate via interface (uses OutputEvent)
            if score >= self.threshold:  # still filter on random score for demo
                event = self.generate_output_event(
                    user_id=user_id,
                    score=score,
                    data=data,
                    model_id=self.model_id
                )
                events.append(event)
                logger.info(f"Detected anomaly for user {user_id} with random score {score}")
        
        return events
