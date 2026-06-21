/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class XmeasFieldMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String fieldName;
    private final int fieldIndex;
    private final String displayName;
    private final MeasurementUnit unit;
    private final double minValidValue;
    private final double maxValidValue;
    private final double typicalMin;
    private final double typicalMax;
    private final double samplingIntervalSeconds;
    private final double deadTimeSeconds;
    private final String measurementGroup;
    private final String streamName;
    private final Map<String, String> dimensions;
    private final boolean required;
    private final double defaultValue;

    private XmeasFieldMetadata(Builder builder) {
        this.fieldName = Objects.requireNonNull(builder.fieldName, "fieldName must not be null");
        this.fieldIndex = builder.fieldIndex;
        this.displayName = builder.displayName != null ? builder.displayName : builder.fieldName;
        this.unit = builder.unit != null ? builder.unit : MeasurementUnit.UNKNOWN;
        this.minValidValue = builder.minValidValue;
        this.maxValidValue = builder.maxValidValue;
        this.typicalMin = builder.typicalMin;
        this.typicalMax = builder.typicalMax;
        this.samplingIntervalSeconds = builder.samplingIntervalSeconds;
        this.deadTimeSeconds = builder.deadTimeSeconds;
        this.measurementGroup = builder.measurementGroup;
        this.streamName = builder.streamName;
        this.dimensions = builder.dimensions != null ? new HashMap<>(builder.dimensions) : new HashMap<>();
        this.required = builder.required;
        this.defaultValue = builder.defaultValue;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getFieldIndex() {
        return fieldIndex;
    }

    public String getDisplayName() {
        return displayName;
    }

    public MeasurementUnit getUnit() {
        return unit;
    }

    public double getMinValidValue() {
        return minValidValue;
    }

    public double getMaxValidValue() {
        return maxValidValue;
    }

    public double getTypicalMin() {
        return typicalMin;
    }

    public double getTypicalMax() {
        return typicalMax;
    }

    public double getSamplingIntervalSeconds() {
        return samplingIntervalSeconds;
    }

    public double getDeadTimeSeconds() {
        return deadTimeSeconds;
    }

    public String getMeasurementGroup() {
        return measurementGroup;
    }

    public String getStreamName() {
        return streamName;
    }

    public Map<String, String> getDimensions() {
        return new HashMap<>(dimensions);
    }

    public boolean isRequired() {
        return required;
    }

    public double getDefaultValue() {
        return defaultValue;
    }

    public boolean isValidValue(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return false;
        }
        return value >= minValidValue && value <= maxValidValue;
    }

    public boolean isTypicalValue(double value) {
        return value >= typicalMin && value <= typicalMax;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String fieldName;
        private int fieldIndex;
        private String displayName;
        private MeasurementUnit unit;
        private double minValidValue = -Double.MAX_VALUE;
        private double maxValidValue = Double.MAX_VALUE;
        private double typicalMin = -Double.MAX_VALUE;
        private double typicalMax = Double.MAX_VALUE;
        private double samplingIntervalSeconds;
        private double deadTimeSeconds;
        private String measurementGroup;
        private String streamName;
        private Map<String, String> dimensions;
        private boolean required = false;
        private double defaultValue = Double.NaN;

        public Builder fieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder fieldIndex(int fieldIndex) {
            this.fieldIndex = fieldIndex;
            return this;
        }

        public Builder displayName(String displayName) {
            this.displayName = displayName;
            return this;
        }

        public Builder unit(MeasurementUnit unit) {
            this.unit = unit;
            return this;
        }

        public Builder minValidValue(double minValidValue) {
            this.minValidValue = minValidValue;
            return this;
        }

        public Builder maxValidValue(double maxValidValue) {
            this.maxValidValue = maxValidValue;
            return this;
        }

        public Builder typicalMin(double typicalMin) {
            this.typicalMin = typicalMin;
            return this;
        }

        public Builder typicalMax(double typicalMax) {
            this.typicalMax = typicalMax;
            return this;
        }

        public Builder samplingIntervalSeconds(double samplingIntervalSeconds) {
            this.samplingIntervalSeconds = samplingIntervalSeconds;
            return this;
        }

        public Builder deadTimeSeconds(double deadTimeSeconds) {
            this.deadTimeSeconds = deadTimeSeconds;
            return this;
        }

        public Builder measurementGroup(String measurementGroup) {
            this.measurementGroup = measurementGroup;
            return this;
        }

        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }

        public Builder dimensions(Map<String, String> dimensions) {
            this.dimensions = dimensions;
            return this;
        }

        public Builder addDimension(String key, String value) {
            if (this.dimensions == null) {
                this.dimensions = new HashMap<>();
            }
            this.dimensions.put(key, value);
            return this;
        }

        public Builder required(boolean required) {
            this.required = required;
            return this;
        }

        public Builder defaultValue(double defaultValue) {
            this.defaultValue = defaultValue;
            return this;
        }

        public XmeasFieldMetadata build() {
            return new XmeasFieldMetadata(this);
        }
    }
}
