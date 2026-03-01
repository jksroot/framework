// Package schema handles dynamic schema loading from JSON files and message validation.
package schema

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// SchemaDef represents a schema definition loaded from a JSON file.
type SchemaDef struct {
	Name     string            `json:"name"`
	Fields   map[string]string `json:"fields"`
	Required []string          `json:"required"`
}

// Validator handles loading schema definitions and validating messages.
type Validator struct {
	schemas map[string]*SchemaDef
}

// NewValidator creates a Validator and loads all schema JSON files from the given directory.
func NewValidator(schemasPath string) (*Validator, error) {
	v := &Validator{
		schemas: make(map[string]*SchemaDef),
	}
	if err := v.loadSchemas(schemasPath); err != nil {
		return nil, err
	}
	return v, nil
}

// loadSchemas loads all *.json files from the schemas directory.
func (v *Validator) loadSchemas(schemasPath string) error {
	info, err := os.Stat(schemasPath)
	if err != nil || !info.IsDir() {
		log.Printf("WARNING: Schemas directory not found: %s", schemasPath)
		return nil
	}

	entries, err := os.ReadDir(schemasPath)
	if err != nil {
		return fmt.Errorf("failed to read schemas directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		schemaName := strings.TrimSuffix(entry.Name(), ".json")
		filePath := filepath.Join(schemasPath, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("WARNING: Failed to read schema file %s: %v", filePath, err)
			continue
		}
		var schemaDef SchemaDef
		if err := json.Unmarshal(data, &schemaDef); err != nil {
			log.Printf("WARNING: Failed to parse schema file %s: %v", filePath, err)
			continue
		}
		v.schemas[schemaName] = &schemaDef
		log.Printf("Loaded schema: %s", schemaName)
	}
	return nil
}

// GetSchema returns the schema definition for the given name.
func (v *Validator) GetSchema(schemaName string) *SchemaDef {
	return v.schemas[schemaName]
}

// ValidateMessage validates a message (map) against the named schema.
// Returns the validated/coerced message map, or an error if validation fails.
func (v *Validator) ValidateMessage(message map[string]interface{}, schemaName string) (map[string]interface{}, error) {
	schemaDef := v.schemas[schemaName]
	if schemaDef == nil {
		return nil, fmt.Errorf("schema not found: %s", schemaName)
	}

	// Check required fields
	for _, req := range schemaDef.Required {
		val, ok := message[req]
		if !ok || val == nil {
			return nil, fmt.Errorf("missing required field: %s", req)
		}
	}

	// Validate and coerce field types
	validated := make(map[string]interface{})
	for fieldName, fieldType := range schemaDef.Fields {
		val, ok := message[fieldName]
		if !ok {
			// Optional field not present
			validated[fieldName] = nil
			continue
		}
		coerced, err := coerceValue(val, fieldType)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", fieldName, err)
		}
		validated[fieldName] = coerced
	}
	return validated, nil
}

// coerceValue attempts to coerce a value to the expected type string.
func coerceValue(val interface{}, expectedType string) (interface{}, error) {
	switch expectedType {
	case "str":
		switch v := val.(type) {
		case string:
			return v, nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	case "int":
		switch v := val.(type) {
		case float64:
			return int(v), nil
		case int:
			return v, nil
		case string:
			i, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to int", v)
			}
			return i, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to int", val)
		}
	case "float":
		switch v := val.(type) {
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case string:
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to float", v)
			}
			return f, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to float", val)
		}
	case "bool":
		switch v := val.(type) {
		case bool:
			return v, nil
		case string:
			b, err := strconv.ParseBool(v)
			if err != nil {
				return nil, fmt.Errorf("cannot convert %q to bool", v)
			}
			return b, nil
		default:
			return nil, fmt.Errorf("cannot convert %T to bool", val)
		}
	default:
		// Unknown type, pass through as string
		return fmt.Sprintf("%v", val), nil
	}
}
