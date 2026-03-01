import polars as pl
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

class EventProcessor:
    """Converts batch of validated messages to Polars DataFrame."""
    
    def process_batch(self, validated_messages: List[Dict[str, Any]]) -> pl.DataFrame:
        """Convert list of validated message dicts to Polars DF."""
        if not validated_messages:
            return pl.DataFrame()
        try:
            # Extract fields dynamically
            df = pl.from_dicts(validated_messages)
            logger.debug(f"Created DataFrame with {len(df)} rows and columns: {df.columns}")
            return df
        except Exception as e:
            logger.error(f"Failed to create DataFrame: {str(e)}")
            return pl.DataFrame()
