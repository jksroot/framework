import json
from pathlib import Path
from typing import Dict, Any, Type, List, Optional
from pydantic import BaseModel, Field, create_model, UUID4
import logging

logger = logging.getLogger(__name__)

class SchemaValidator:
    """Handles dynamic Pydantic model creation from schema files and validation."""
    
    def __init__(self, schemas_path: str = "schemas"):
        self.schemas_path = Path(schemas_path)
        self.models: Dict[str, Type[BaseModel]] = {}
        self._load_schemas()
    
    def _load_schemas(self):
        """Load all schema definitions from JSON files."""
        if not self.schemas_path.exists():
            logger.warning(f"Schemas directory not found: {self.schemas_path}")
            return
        for schema_file in self.schemas_path.glob("*.json"):
            schema_name = schema_file.stem
            with open(schema_file, 'r') as f:
                schema_def = json.load(f)
            self.models[schema_name] = self._create_pydantic_model(schema_name, schema_def)
            logger.info(f"Loaded schema: {schema_name}")
    
    def _create_pydantic_model(self, name: str, schema_def: Dict[str, Any]) -> Type[BaseModel]:
        """Dynamically create Pydantic model from schema definition."""
        fields = {}
        field_types = {
            "str": str,
            "int": int,
            "float": float,
            "bool": bool,
            # Add more as needed
        }
        
        for field_name, field_type_str in schema_def.get("fields", {}).items():
            field_type = field_types.get(field_type_str, str)  # default to str
            if field_name == "event_id":
                field_type = UUID4  # special for UUID
            # Make optional unless required
            required = field_name in schema_def.get("required", [])
            if required:
                fields[field_name] = (field_type, Field(...))
            else:
                fields[field_name] = (Optional[field_type], Field(None))
        
        model = create_model(name, **fields)
        return model
    
    def get_model(self, schema_name: str) -> Optional[Type[BaseModel]]:
        """Get Pydantic model for schema."""
        return self.models.get(schema_name)
    
    def validate_message(self, message: Dict[str, Any], schema_name: str = "app_log_type_login") -> Optional[BaseModel]:
        """Validate message against schema. Return validated model or None if invalid."""
        model_cls = self.get_model(schema_name)
        if not model_cls:
            logger.error(f"Schema not found: {schema_name}")
            return None
        try:
            # Parse and validate
            validated = model_cls(**message)
            return validated
        except Exception as e:
            logger.warning(f"Validation failed for message: {str(e)}")
            return None
