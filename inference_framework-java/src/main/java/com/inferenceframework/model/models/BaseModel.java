package com.inferenceframework.model.models;  // model sub-module

import com.inferenceframework.event.OutputEvent;
import com.inferenceframework.model.ModelInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base implementation of ModelInterface.
 * Adapted for Java: uses List<Map<String, Object>> instead of Polars DataFrame.
 */
public abstract class BaseModel implements ModelInterface {
    protected static final Logger logger = LoggerFactory.getLogger(BaseModel.class);

    protected Map<String, Object> config;
    protected String modelId;

    /**
     * Constructor.
     */
    public BaseModel(Map<String, Object> config) {
        this.config = config != null ? config : new HashMap<>();
        this.modelId = this.getClass().getSimpleName().toLowerCase();
    }

    /**
     * Default process - should be overridden by subclasses.
     * In base, logs warning and returns empty list.
     */
    @Override
    public List<OutputEvent> process(List<Map<String, Object>> messages, Map<String, Map<String, Object>> features) {
        logger.warn("BaseModel process called - override in subclass");
        return new ArrayList<>();
    }
}