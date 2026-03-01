package com.inferenceframework.schema;  // schema module

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Handles dynamic schema validation using JSON Schema Validator library (networknt).
 * Converts custom schema files to standard JSON Schema format for validation.
 * Appropriate library for JSON schema validation in Java.
 */
public class SchemaValidator {
    private static final Logger logger = LoggerFactory.getLogger(SchemaValidator.class);

    private final String schemasPath;
    private final ObjectMapper mapper;
    private final Map<String, JsonSchema> schemas;  // schema_name -> JsonSchema

    /**
     * Constructor loads schemas from directory.
     */
    public SchemaValidator(String schemasPath) {
        this.schemasPath = schemasPath;
        this.mapper = new ObjectMapper();
        this.schemas = new HashMap<>();
        loadSchemas();
    }

    /**
     * Default constructor.
     */
    public SchemaValidator() {
        this("schemas");
    }

    /**
     * Load all schema definitions from JSON files and convert to JsonSchema.
     */
    private void loadSchemas() {
        Path dir = Paths.get(schemasPath);
        if (!Files.exists(dir)) {
            logger.warn("Schemas directory not found: {}", schemasPath);
            return;
        }
        try {
            Files.list(dir)
                .filter(p -> p.toString().endsWith(".json"))
                .forEach(this::loadSchema);
        } catch (IOException e) {
            logger.error("Failed to list schemas", e);
        }
    }

    /**
     * Load and convert one schema file to standard JSON Schema for validation.
     */
    private void loadSchema(Path schemaFile) {
        String schemaName = schemaFile.getFileName().toString().replace(".json", "");
        try {
            // Read custom schema def
            JsonNode customSchemaDef = mapper.readTree(schemaFile.toFile());
            // Convert to standard JSON Schema (Draft 2020-12 via lib)
            JsonNode jsonSchemaNode = convertToJsonSchema(customSchemaDef);
            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
            JsonSchema schema = factory.getSchema(jsonSchemaNode);
            schemas.put(schemaName, schema);
            logger.info("Loaded schema: {}", schemaName);
        } catch (Exception e) {
            logger.error("Failed to load schema: {}", schemaName, e);
        }
    }

    /**
     * Convert custom schema format to standard JSON Schema object.
     * Custom: {"fields": {"field": "type"}, "required": [...]}
     * Standard: {"type": "object", "properties": {...}, "required": [...]}
     */
    private JsonNode convertToJsonSchema(JsonNode customDef) {
        ObjectMapper om = new ObjectMapper();
        Map<String, Object> stdSchema = new HashMap<>();
        stdSchema.put("type", "object");
        stdSchema.put("additionalProperties", true);  // allow extra

        Map<String, Object> properties = new HashMap<>();
        JsonNode fields = customDef.get("fields");
        if (fields != null && fields.isObject()) {
            fields.fields().forEachRemaining(entry -> {
                String typeStr = entry.getValue().asText();
                Map<String, Object> propType = new HashMap<>();
                propType.put("type", mapType(typeStr));
                properties.put(entry.getKey(), propType);
            });
        }
        stdSchema.put("properties", properties);

        // Required fields
        JsonNode required = customDef.get("required");
        if (required != null && required.isArray()) {
            List<String> reqList = new ArrayList<>();
            required.forEach(n -> reqList.add(n.asText()));
            stdSchema.put("required", reqList);
        }

        return om.valueToTree(stdSchema);
    }

    /**
     * Map simple type strings to JSON Schema types.
     */
    private String mapType(String typeStr) {
        switch (typeStr.toLowerCase()) {
            case "str":
            case "string": return "string";
            case "int":
            case "integer": return "integer";
            case "float":
            case "number": return "number";
            case "bool":
            case "boolean": return "boolean";
            default: return "string";
        }
    }

    /**
     * Get schema for name.
     */
    public JsonSchema getSchema(String schemaName) {
        return schemas.get(schemaName);
    }

    /**
     * Validate message (Map/JsonNode) against schema.
     * Return validated map (as-is if valid) or null if invalid.
     * For compatibility with Python version.
     */
    public Map<String, Object> validateMessage(Map<String, Object> message, String schemaName) {
        JsonSchema schema = getSchema(schemaName);
        if (schema == null) {
            logger.error("Schema not found: {}", schemaName);
            return null;
        }
        try {
            // Convert message to JsonNode for validation
            JsonNode jsonNode = mapper.valueToTree(message);
            Set<ValidationMessage> errors = schema.validate(jsonNode);
            if (!errors.isEmpty()) {
                logger.warn("Validation failed for message: {}", errors);
                return null;
            }
            // Valid: return original map
            return message;
        } catch (Exception e) {
            logger.warn("Validation error: {}", e.getMessage());
            return null;
        }
    }
}