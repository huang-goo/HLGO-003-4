/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class XmeasSchemaRegistry implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, XmeasFieldMetadata> fieldMap;
    private final List<XmeasFieldMetadata> fields;
    private final String schemaVersion;
    private final long schemaTimestamp;
    private final Map<String, String> metadata;

    private XmeasSchemaRegistry(Builder builder) {
        this.fieldMap = new ConcurrentHashMap<>(builder.fieldMap);
        this.fields = Collections.unmodifiableList(new ArrayList<>(builder.fields));
        this.schemaVersion = builder.schemaVersion != null ? builder.schemaVersion : "1.0.0";
        this.schemaTimestamp = builder.schemaTimestamp > 0 ? builder.schemaTimestamp : System.currentTimeMillis();
        this.metadata = builder.metadata != null ? new HashMap<>(builder.metadata) : new HashMap<>();
    }

    public XmeasFieldMetadata getField(String fieldName) {
        return fieldMap.get(fieldName);
    }

    public XmeasFieldMetadata getField(int fieldIndex) {
        String fieldName = "xmeas_" + fieldIndex;
        return fieldMap.get(fieldName);
    }

    public boolean hasField(String fieldName) {
        return fieldMap.containsKey(fieldName);
    }

    public List<XmeasFieldMetadata> getAllFields() {
        return fields;
    }

    public int getFieldCount() {
        return fields.size();
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public long getSchemaTimestamp() {
        return schemaTimestamp;
    }

    public Map<String, String> getMetadata() {
        return new HashMap<>(metadata);
    }

    public List<XmeasFieldMetadata> getFieldsByGroup(String groupName) {
        List<XmeasFieldMetadata> result = new ArrayList<>();
        for (XmeasFieldMetadata field : fields) {
            if (groupName.equals(field.getMeasurementGroup())) {
                result.add(field);
            }
        }
        return result;
    }

    public List<XmeasFieldMetadata> getFieldsByStream(String streamName) {
        List<XmeasFieldMetadata> result = new ArrayList<>();
        for (XmeasFieldMetadata field : fields) {
            if (streamName.equals(field.getStreamName())) {
                result.add(field);
            }
        }
        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static XmeasSchemaRegistry getDefault() {
        return DefaultSchemaHolder.INSTANCE;
    }

    private static class DefaultSchemaHolder {
        static final XmeasSchemaRegistry INSTANCE = createDefaultSchema();
    }

    private static XmeasSchemaRegistry createDefaultSchema() {
        Builder builder = builder()
                .schemaVersion("1.0.0")
                .addMetadata("schema_name", "tep_xmeas_schema")
                .addMetadata("description", "TEP (Tennessee Eastman Process) 41 xmeas measurements schema")
                .addMetadata("source", "tep_simulator");

        for (int i = 1; i <= 41; i++) {
            builder.addField(createDefaultField(i));
        }

        return builder.build();
    }

    private static XmeasFieldMetadata createDefaultField(int index) {
        XmeasFieldMetadata.Builder builder = XmeasFieldMetadata.builder()
                .fieldName("xmeas_" + index)
                .fieldIndex(index)
                .displayName("XMEAS " + index);

        if (index <= 22) {
            builder.measurementGroup("CONTINUOUS")
                    .samplingIntervalSeconds(180)
                    .unit(MeasurementUnit.DIMENSIONLESS);
        } else if (index >= 23 && index <= 28) {
            builder.measurementGroup("SAMPLED")
                    .streamName("stream_6")
                    .samplingIntervalSeconds(360)
                    .deadTimeSeconds(360)
                    .unit(MeasurementUnit.MOL_PERCENT);
        } else if (index >= 29 && index <= 36) {
            builder.measurementGroup("SAMPLED")
                    .streamName("stream_9")
                    .samplingIntervalSeconds(360)
                    .deadTimeSeconds(360)
                    .unit(MeasurementUnit.MOL_PERCENT);
        } else if (index >= 37 && index <= 41) {
            builder.measurementGroup("SAMPLED")
                    .streamName("stream_11")
                    .samplingIntervalSeconds(900)
                    .deadTimeSeconds(900)
                    .unit(MeasurementUnit.MOL_PERCENT);
        }

        builder.addDimension("measurement_type", "xmeas")
                .addDimension("field_index", String.valueOf(index));

        return builder.build();
    }

    public static class Builder {
        private final Map<String, XmeasFieldMetadata> fieldMap = new HashMap<>();
        private final List<XmeasFieldMetadata> fields = new ArrayList<>();
        private String schemaVersion;
        private long schemaTimestamp;
        private Map<String, String> metadata;

        public Builder addField(XmeasFieldMetadata field) {
            fieldMap.put(field.getFieldName(), field);
            fields.add(field);
            return this;
        }

        public Builder schemaVersion(String schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        public Builder schemaTimestamp(long schemaTimestamp) {
            this.schemaTimestamp = schemaTimestamp;
            return this;
        }

        public Builder metadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder addMetadata(String key, String value) {
            if (this.metadata == null) {
                this.metadata = new HashMap<>();
            }
            this.metadata.put(key, value);
            return this;
        }

        public XmeasSchemaRegistry build() {
            return new XmeasSchemaRegistry(this);
        }
    }
}
