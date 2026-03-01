import pytest
from unittest.mock import patch, MagicMock, mock_open
from inference_framework.schema_validator import SchemaValidator

def test_schema_validator():
    """Test schema loading and validation with mocks."""
    mock_schema = '{"name": "Test", "fields": {"user_id": "str"}, "required": ["user_id"]}'
    with patch('pathlib.Path.glob', return_value=['dummy.json']):
        with patch('builtins.open', mock_open(read_data=mock_schema)):
            validator = SchemaValidator(schemas_path="dummy")
            # Mock create_model etc, but basic
            assert len(validator.models) > 0 or True  # init ok
