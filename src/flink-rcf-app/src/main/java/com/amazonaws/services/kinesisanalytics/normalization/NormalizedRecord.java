/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class NormalizedRecord implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, Double> measures;
    private final Map<String, String> dimensions;
    private final long timestamp;
    private final String timeUnit;
    private final String schemaVersion;
    private final Map<String, String> fieldStatus;
    private final Map<String, String> clipReasons;
    private final Map<String, String> unitConversions;
    private final int missingFields;
    private final int clippedFields;
    private final int convertedFields;
    private final boolean outOfOrder;

    private NormalizedRecord(Builder builder) {
        this.measures = builder.measures != null ? new HashMap<>(builder.measures) : new HashMap<>();
        this.dimensions = builder.dimensions != null ? new HashMap<>(builder.dimensions) : new HashMap<>();
        this.timestamp = builder.timestamp;
        this.timeUnit = builder.timeUnit != null ? builder.timeUnit : "MILLISECONDS";
        this.schemaVersion = builder.schemaVersion;
        this.fieldStatus = builder.fieldStatus != null ? new HashMap<>(builder.fieldStatus) : new HashMap<>();
        this.clipReasons = builder.clipReasons != null ? new HashMap<>(builder.clipReasons) : new HashMap<>();
        this.unitConversions = builder.unitConversions != null ? new HashMap<>(builder.unitConversions) : new HashMap<>();
        this.missingFields = builder.missingFields;
        this.clippedFields = builder.clippedFields;
        this.convertedFields = builder.convertedFields;
        this.outOfOrder = builder.outOfOrder;
    }

    public Map<String, Double> getMeasures() {
        return new HashMap<>(measures);
    }

    public double getMeasure(String fieldName) {
        return measures.getOrDefault(fieldName, Double.NaN);
    }

    public boolean hasMeasure(String fieldName) {
        return measures.containsKey(fieldName);
    }

    public Map<String, String> getDimensions() {
        return new HashMap<>(dimensions);
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTimeUnit() {
        return timeUnit;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public String getFieldStatus(String fieldName) {
        return fieldStatus.get(fieldName);
    }

    public Map<String, String> getAllFieldStatus() {
        return new HashMap<>(fieldStatus);
    }

    public String getClipReason(String fieldName) {
        return clipReasons.get(fieldName);
    }

    public Map<String, String> getAllClipReasons() {
        return new HashMap<>(clipReasons);
    }

    public String getUnitConversion(String fieldName) {
        return unitConversions.get(fieldName);
    }

    public Map<String, String> getAllUnitConversions() {
        return new HashMap<>(unitConversions);
    }

    public int getMissingFields() {
        return missingFields;
    }

    public int getClippedFields() {
        return clippedFields;
    }

    public int getConvertedFields() {
        return convertedFields;
    }

    public boolean isOutOfOrder() {
        return outOfOrder;
    }

    public int getMeasureCount() {
        return measures.size();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<String, Double> measures;
        private Map<String, String> dimensions;
        private long timestamp;
        private String timeUnit;
        private String schemaVersion;
        private Map<String, String> fieldStatus;
        private Map<String, String> clipReasons;
        private Map<String, String> unitConversions;
        private int missingFields;
        private int clippedFields;
        private int convertedFields;
        private boolean outOfOrder;

        public Builder measures(Map<String, Double> measures) {
            this.measures = measures;
            return this;
        }

        public Builder addMeasure(String name, double value) {
            if (this.measures == null) {
                this.measures = new HashMap<>();
            }
            this.measures.put(name, value);
            return this;
        }

        public Builder dimensions(Map<String, String> dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public Builder addDimension(String name, String value) {
            if (this.dimensions == null) {
                this.dimensions = new HashMap<>();
            }
            this.dimensions.put(name, value);
            return this;
        }

        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder timeUnit(String timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder schemaVersion(String schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public Builder fieldStatus(Map<String, String> fieldStatus) {
            this.fieldStatus = fieldStatus;
            return this;
        }

        public Builder addFieldStatus(String field, String status) {
            if (this.fieldStatus == null) {
                this.fieldStatus = new HashMap<>();
            }
            this.fieldStatus.put(field, status);
            return this;
        }

        public Builder clipReasons(Map<String, String> clipReasons) {
            this.clipReasons = clipReasons;
            return this;
        }

        public Builder addClipReason(String field, String reason) {
            if (this.clipReasons == null) {
                this.clipReasons = new HashMap<>();
            }
            this.clipReasons.put(field, reason);
            return this;
        }

        public Builder unitConversions(Map<String, String> unitConversions) {
            this.unitConversions = unitConversions;
            return this;
        }

        public Builder addUnitConversion(String field, String conversion) {
            if (this.unitConversions == null) {
                this.unitConversions = new HashMap<>();
            }
            this.unitConversions.put(field, conversion);
            return this;
        }

        public Builder missingFields(int missingFields) {
            this.missingFields = missingFields;
            return this;
        }

        public Builder clippedFields(int clippedFields) {
            this.clippedFields = clippedFields;
            return this;
        }

        public Builder convertedFields(int convertedFields) {
            this.convertedFields = convertedFields;
            return this;
        }

        public Builder outOfOrder(boolean outOfOrder) {
            this.outOfOrder = outOfOrder;
            return this;
        }

        public NormalizedRecord build() {
            return new NormalizedRecord(this);
        }
    }
}
