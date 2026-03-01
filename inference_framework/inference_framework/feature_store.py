import json
from pathlib import Path
from typing import Dict, Any, Optional, List
import logging

logger = logging.getLogger(__name__)

class FeatureStore:
    """Manages loading and retrieval of feature vectors for user_ids from JSON file."""
    
    def __init__(self, features_path: str):
        self.features_path = Path(features_path)
        self.features: Dict[str, Dict[str, Any]] = self._load_features()
    
    def _load_features(self) -> Dict[str, Dict[str, Any]]:
        """Load features from JSON file."""
        if not self.features_path.exists():
            logger.warning(f"Features file not found: {self.features_path}. Using empty store.")
            return {}
        try:
            with open(self.features_path, 'r') as f:
                features = json.load(f)
            logger.info(f"Loaded features for {len(features)} users.")
            return features
        except Exception as e:
            logger.error(f"Failed to load features: {str(e)}")
            return {}
    
    def get_feature_vector(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get feature vector for a user_id."""
        return self.features.get(user_id)
    
    def get_features_for_users(self, user_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get features for multiple user_ids."""
        return {uid: self.features.get(uid, {}) for uid in user_ids}
