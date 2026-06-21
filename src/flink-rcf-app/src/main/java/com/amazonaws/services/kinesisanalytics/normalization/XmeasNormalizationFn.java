/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import com.amazonaws.services.timestream.TimestreamPoint;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@SuppressWarnings("serial")
public class XmeasNormalizationFn extends RichFlatMapFunction<String, List<TimestreamPoint>> {

    private static final Logger logger = LoggerFactory.getLogger(XmeasNormalizationFn.class);

    private final ParameterTool parameter;
    private transient JsonNormalizationProcessor processor;
    private transient NormalizedToTimestreamConverter converter;

    public XmeasNormalizationFn(ParameterTool parameter) {
        this.parameter = parameter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        XmeasSchemaRegistry schemaRegistry = XmeasSchemaRegistry.getDefault();

        boolean enableOutOfOrder = parameter.getBoolean("enableOutOfOrderHandling", false);
        long maxOutOfOrderMs = parameter.getLong("maxOutOfOrderMs", 5000L);
        int maxBufferedRecords = parameter.getInt("maxBufferedRecords", 1000);

        boolean fillMissingFields = parameter.getBoolean("fillMissingFields", true);
        String timestampField = parameter.get("timestampField", "timestamp");

        OutlierClipper.ClipStrategy clipStrategy = OutlierClipper.ClipStrategy.valueOf(
                parameter.get("clipStrategy", "CLAMP_TO_BOUNDARY"));
        boolean useTypicalRange = parameter.getBoolean("useTypicalRange", false);
        double stdDevMultiplier = parameter.getDouble("stdDevMultiplier", 3.0);
        boolean enableStatisticalClipping = parameter.getBoolean("enableStatisticalClipping", false);

        OutlierClipper outlierClipper = new OutlierClipper(
                clipStrategy, useTypicalRange, stdDevMultiplier, enableStatisticalClipping);

        OutOfOrderManager outOfOrderManager = null;
        if (enableOutOfOrder) {
            outOfOrderManager = new OutOfOrderManager(maxOutOfOrderMs, maxBufferedRecords);
        }

        UnitConverter unitConverter = new UnitConverter();

        processor = JsonNormalizationProcessor.builder()
                .schemaRegistry(schemaRegistry)
                .unitConverter(unitConverter)
                .outlierClipper(outlierClipper)
                .outOfOrderManager(outOfOrderManager)
                .timestampField(timestampField)
                .enableOutOfOrderHandling(enableOutOfOrder)
                .fillMissingFields(fillMissingFields)
                .build();

        boolean includeMetadataDimensions = parameter.getBoolean("includeMetadataDimensions", true);
        boolean includeStatusAsMeasure = parameter.getBoolean("includeStatusAsMeasure", false);
        String schemaVersionDimension = parameter.get("schemaVersionDimension", "schema_version");

        converter = new NormalizedToTimestreamConverter(
                includeMetadataDimensions, includeStatusAsMeasure, schemaVersionDimension);

        logger.info("XmeasNormalizationFn initialized with schema version: {}",
                schemaRegistry.getSchemaVersion());
    }

    @Override
    public void flatMap(String jsonString, Collector<List<TimestreamPoint>> out) throws Exception {
        List<NormalizedRecord> records = processor.process(jsonString);

        for (NormalizedRecord record : records) {
            List<TimestreamPoint> points = converter.convert(record);
            if (!points.isEmpty()) {
                out.collect(points);
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (processor != null) {
            List<NormalizedRecord> flushed = processor.flush();
            if (!flushed.isEmpty()) {
                logger.info("Flushed {} records on close", flushed.size());
            }
        }
        super.close();
    }

    public JsonNormalizationProcessor getProcessor() {
        return processor;
    }
}
