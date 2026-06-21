/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonNormalizationProcessor implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(JsonNormalizationProcessor.class);

    private final XmeasSchemaRegistry schemaRegistry;
    private final UnitConverter unitConverter;
    private final OutlierClipper outlierClipper;
    private final OutOfOrderManager outOfOrderManager;
    private final Gson gson;
    private final String timestampField;
    private final boolean enableOutOfOrderHandling;
    private final boolean fillMissingFields;
    private final MeasurementUnit targetUnit;

    private long processedCount = 0;
    private long errorCount = 0;

    public JsonNormalizationProcessor(XmeasSchemaRegistry schemaRegistry,
                                      UnitConverter unitConverter,
                                      OutlierClipper outlierClipper,
                                      OutOfOrderManager outOfOrderManager,
                                      String timestampField,
                                      boolean enableOutOfOrderHandling,
                                      boolean fillMissingFields,
                                      MeasurementUnit targetUnit) {
        this.schemaRegistry = schemaRegistry;
        this.unitConverter = unitConverter;
        this.outlierClipper = outlierClipper;
        this.outOfOrderManager = outOfOrderManager;
        this.gson = new Gson();
        this.timestampField = timestampField;
        this.enableOutOfOrderHandling = enableOutOfOrderHandling;
        this.fillMissingFields = fillMissingFields;
        this.targetUnit = targetUnit;
    }

    public List<NormalizedRecord> process(String jsonString) {
        processedCount++;

        try {
            JsonObject jsonObject = gson.fromJson(jsonString, JsonObject.class);
            if (jsonObject == null) {
                errorCount++;
                logger.warn("Null JSON object parsed from input");
                return java.util.Collections.emptyList();
            }

            NormalizedRecord record = normalizeRecord(jsonObject);

            if (enableOutOfOrderHandling && outOfOrderManager != null) {
                return outOfOrderManager.processRecord(record);
            } else {
                return java.util.Collections.singletonList(record);
            }

        } catch (JsonParseException e) {
            errorCount++;
            logger.warn("Failed to parse JSON: {}", e.getMessage());
            return java.util.Collections.emptyList();
        } catch (Exception e) {
            errorCount++;
            logger.error("Error processing JSON record", e);
            return java.util.Collections.emptyList();
        }
    }

    private NormalizedRecord normalizeRecord(JsonObject jsonObject) {
        NormalizedRecord.Builder builder = NormalizedRecord.builder();
        builder.schemaVersion(schemaRegistry.getSchemaVersion());

        long timestamp = extractTimestamp(jsonObject);
        builder.timestamp(timestamp);
        builder.timeUnit("MILLISECONDS");

        Map<String, Double> measures = new HashMap<>();
        Map<String, String> fieldStatus = new HashMap<>();
        Map<String, String> clipReasons = new HashMap<>();
        Map<String, String> unitConversions = new HashMap<>();
        Map<String, String> dimensions = new HashMap<>();

        int missingCount = 0;
        int clippedCount = 0;
        int convertedCount = 0;

        for (XmeasFieldMetadata fieldMetadata : schemaRegistry.getAllFields()) {
            String fieldName = fieldMetadata.getFieldName();
            JsonElement element = jsonObject.get(fieldName);

            if (element == null || element.isJsonNull()) {
                if (fieldMetadata.isRequired()) {
                    missingCount++;
                    fieldStatus.put(fieldName, "missing_required");
                    if (fillMissingFields && !Double.isNaN(fieldMetadata.getDefaultValue())) {
                        measures.put(fieldName, fieldMetadata.getDefaultValue());
                        fieldStatus.put(fieldName, "filled_default");
                    }
                } else {
                    missingCount++;
                    fieldStatus.put(fieldName, "missing_optional");
                    if (fillMissingFields && !Double.isNaN(fieldMetadata.getDefaultValue())) {
                        measures.put(fieldName, fieldMetadata.getDefaultValue());
                        fieldStatus.put(fieldName, "filled_default");
                    }
                }
                continue;
            }

            double value;
            try {
                if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isNumber()) {
                    value = element.getAsDouble();
                } else {
                    value = Double.parseDouble(element.getAsString());
                }
            } catch (NumberFormatException e) {
                fieldStatus.put(fieldName, "parse_error");
                missingCount++;
                continue;
            }

            if (targetUnit != null && fieldMetadata.getUnit() != MeasurementUnit.UNKNOWN
                    && fieldMetadata.getUnit() != targetUnit) {
                if (unitConverter.canConvert(fieldMetadata.getUnit(), targetUnit)) {
                    double converted = unitConverter.convert(value, fieldMetadata.getUnit(), targetUnit);
                    unitConversions.put(fieldName, fieldMetadata.getUnit().getName() + "->" + targetUnit.getName());
                    value = converted;
                    convertedCount++;
                }
            }

            OutlierClipper.ClipResult clipResult = outlierClipper.clip(fieldName, value, fieldMetadata);
            if (clipResult.isClipped()) {
                clippedCount++;
                clipReasons.put(fieldName, clipResult.getReason());
                fieldStatus.put(fieldName, "clipped");
            } else {
                fieldStatus.put(fieldName, "normal");
            }

            measures.put(fieldName, clipResult.getValue());
        }

        for (XmeasFieldMetadata fieldMetadata : schemaRegistry.getAllFields()) {
            Map<String, String> fieldDims = fieldMetadata.getDimensions();
            if (fieldDims != null) {
                dimensions.putAll(fieldDims);
            }
        }

        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            String key = entry.getKey();
            if (!schemaRegistry.hasField(key) && !key.equals(timestampField)) {
                if (entry.getValue().isJsonPrimitive() && entry.getValue().getAsJsonPrimitive().isString()) {
                    dimensions.put(key, entry.getValue().getAsString());
                }
            }
        }

        builder.measures(measures)
                .dimensions(dimensions)
                .fieldStatus(fieldStatus)
                .clipReasons(clipReasons)
                .unitConversions(unitConversions)
                .missingFields(missingCount)
                .clippedFields(clippedCount)
                .convertedFields(convertedCount);

        return builder.build();
    }

    private long extractTimestamp(JsonObject jsonObject) {
        if (timestampField != null && jsonObject.has(timestampField)) {
            JsonElement tsElement = jsonObject.get(timestampField);
            if (tsElement != null && !tsElement.isJsonNull()) {
                try {
                    if (tsElement.isJsonPrimitive() && tsElement.getAsJsonPrimitive().isNumber()) {
                        return tsElement.getAsLong();
                    } else {
                        String tsStr = tsElement.getAsString();
                        try {
                            return Long.parseLong(tsStr);
                        } catch (NumberFormatException e) {
                            return Instant.parse(tsStr).toEpochMilli();
                        }
                    }
                } catch (Exception e) {
                    logger.debug("Failed to parse timestamp field {}, using current time", timestampField);
                }
            }
        }
        return Instant.now().toEpochMilli();
    }

    public List<NormalizedRecord> flush() {
        if (outOfOrderManager != null) {
            return outOfOrderManager.flush();
        }
        return java.util.Collections.emptyList();
    }

    public long getProcessedCount() {
        return processedCount;
    }

    public long getErrorCount() {
        return errorCount;
    }

    public XmeasSchemaRegistry getSchemaRegistry() {
        return schemaRegistry;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private XmeasSchemaRegistry schemaRegistry;
        private UnitConverter unitConverter;
        private OutlierClipper outlierClipper;
        private OutOfOrderManager outOfOrderManager;
        private String timestampField = "timestamp";
        private boolean enableOutOfOrderHandling = false;
        private boolean fillMissingFields = true;
        private MeasurementUnit targetUnit;

        public Builder schemaRegistry(XmeasSchemaRegistry schemaRegistry) {
            this.schemaRegistry = schemaRegistry;
            return this;
        }

        public Builder unitConverter(UnitConverter unitConverter) {
            this.unitConverter = unitConverter;
            return this;
        }

        public Builder outlierClipper(OutlierClipper outlierClipper) {
            this.outlierClipper = outlierClipper;
            return this;
        }

        public Builder outOfOrderManager(OutOfOrderManager outOfOrderManager) {
            this.outOfOrderManager = outOfOrderManager;
            return this;
        }

        public Builder timestampField(String timestampField) {
            this.timestampField = timestampField;
            return this;
        }

        public Builder enableOutOfOrderHandling(boolean enable) {
            this.enableOutOfOrderHandling = enable;
            return this;
        }

        public Builder fillMissingFields(boolean fill) {
            this.fillMissingFields = fill;
            return this;
        }

        public Builder targetUnit(MeasurementUnit unit) {
            this.targetUnit = unit;
            return this;
        }

        public JsonNormalizationProcessor build() {
            if (schemaRegistry == null) {
                schemaRegistry = XmeasSchemaRegistry.getDefault();
            }
            if (unitConverter == null) {
                unitConverter = new UnitConverter();
            }
            if (outlierClipper == null) {
                outlierClipper = new OutlierClipper();
            }
            return new JsonNormalizationProcessor(schemaRegistry, unitConverter, outlierClipper,
                    outOfOrderManager, timestampField, enableOutOfOrderHandling, fillMissingFields, targetUnit);
        }
    }
}
