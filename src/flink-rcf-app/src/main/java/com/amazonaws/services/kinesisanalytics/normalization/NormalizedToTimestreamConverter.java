/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import com.amazonaws.services.timestream.TimestreamPoint;
import com.amazonaws.services.timestreamwrite.model.MeasureValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NormalizedToTimestreamConverter implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(NormalizedToTimestreamConverter.class);

    private final boolean includeMetadataDimensions;
    private final boolean includeStatusAsMeasure;
    private final String schemaVersionDimension;

    public NormalizedToTimestreamConverter() {
        this(true, false, "schema_version");
    }

    public NormalizedToTimestreamConverter(boolean includeMetadataDimensions,
                                           boolean includeStatusAsMeasure,
                                           String schemaVersionDimension) {
        this.includeMetadataDimensions = includeMetadataDimensions;
        this.includeStatusAsMeasure = includeStatusAsMeasure;
        this.schemaVersionDimension = schemaVersionDimension;
    }

    public List<TimestreamPoint> convert(NormalizedRecord record) {
        List<TimestreamPoint> points = new ArrayList<>();
        Map<String, Double> measures = record.getMeasures();
        Map<String, String> baseDimensions = record.getDimensions();

        TimestreamPoint basePoint = new TimestreamPoint();
        basePoint.setTime(record.getTimestamp());
        basePoint.setTimeUnit(record.getTimeUnit());

        for (Map.Entry<String, String> dim : baseDimensions.entrySet()) {
            basePoint.addDimension(dim.getKey(), dim.getValue());
        }

        if (includeMetadataDimensions && record.getSchemaVersion() != null) {
            basePoint.addDimension(schemaVersionDimension, record.getSchemaVersion());
        }

        if (record.isOutOfOrder()) {
            basePoint.addDimension("out_of_order", "true");
        }

        for (Map.Entry<String, Double> entry : measures.entrySet()) {
            String measureName = entry.getKey();
            Double value = entry.getValue();

            if (value == null || Double.isNaN(value) || Double.isInfinite(value)) {
                continue;
            }

            TimestreamPoint point = new TimestreamPoint(basePoint, measureName,
                    String.valueOf(value), MeasureValueType.DOUBLE);
            points.add(point);
        }

        if (includeStatusAsMeasure) {
            addStatusMeasures(record, basePoint, points);
        }

        return points;
    }

    private void addStatusMeasures(NormalizedRecord record, TimestreamPoint basePoint, List<TimestreamPoint> points) {
        Map<String, String> fieldStatus = record.getAllFieldStatus();
        for (Map.Entry<String, String> entry : fieldStatus.entrySet()) {
            String statusMeasureName = entry.getKey() + "_status";
            TimestreamPoint statusPoint = new TimestreamPoint(basePoint, statusMeasureName,
                    entry.getValue(), MeasureValueType.VARCHAR);
            points.add(statusPoint);
        }

        TimestreamPoint missingPoint = new TimestreamPoint(basePoint, "_missing_fields",
                String.valueOf(record.getMissingFields()), MeasureValueType.BIGINT);
        points.add(missingPoint);

        TimestreamPoint clippedPoint = new TimestreamPoint(basePoint, "_clipped_fields",
                String.valueOf(record.getClippedFields()), MeasureValueType.BIGINT);
        points.add(clippedPoint);

        TimestreamPoint convertedPoint = new TimestreamPoint(basePoint, "_converted_fields",
                String.valueOf(record.getConvertedFields()), MeasureValueType.BIGINT);
        points.add(convertedPoint);
    }

    public List<TimestreamPoint> convertBatch(List<NormalizedRecord> records) {
        List<TimestreamPoint> allPoints = new ArrayList<>();
        for (NormalizedRecord record : records) {
            allPoints.addAll(convert(record));
        }
        return allPoints;
    }
}
