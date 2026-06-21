/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

public class OutOfOrderManager implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long maxOutOfOrdernessMs;
    private final int maxBufferedRecords;
    private final PriorityQueue<BufferedRecord> buffer;
    private long lastEmittedTimestamp = Long.MIN_VALUE;
    private long watermark = Long.MIN_VALUE;

    public OutOfOrderManager(long maxOutOfOrdernessMs, int maxBufferedRecords) {
        this.maxOutOfOrdernessMs = maxOutOfOrdernessMs;
        this.maxBufferedRecords = maxBufferedRecords;
        this.buffer = new PriorityQueue<>(Comparator.comparingLong(BufferedRecord::getTimestamp));
    }

    public List<NormalizedRecord> processRecord(NormalizedRecord record) {
        List<NormalizedRecord> emitted = new ArrayList<>();
        long recordTimestamp = record.getTimestamp();

        if (recordTimestamp < lastEmittedTimestamp) {
            NormalizedRecord.Builder builder = NormalizedRecord.builder()
                    .measures(record.getMeasures())
                    .dimensions(record.getDimensions())
                    .timestamp(record.getTimestamp())
                    .timeUnit(record.getTimeUnit())
                    .schemaVersion(record.getSchemaVersion())
                    .fieldStatus(record.getAllFieldStatus())
                    .clipReasons(record.getAllClipReasons())
                    .unitConversions(record.getAllUnitConversions())
                    .missingFields(record.getMissingFields())
                    .clippedFields(record.getClippedFields())
                    .convertedFields(record.getConvertedFields())
                    .outOfOrder(true);

            BufferedRecord bufferedRecord = new BufferedRecord(builder.build());
            buffer.add(bufferedRecord);

            if (buffer.size() > maxBufferedRecords) {
                BufferedRecord oldest = buffer.poll();
                if (oldest != null) {
                    emitted.add(oldest.getRecord());
                    lastEmittedTimestamp = Math.max(lastEmittedTimestamp, oldest.getTimestamp());
                }
            }

            emitReadyRecords(emitted);
            return emitted;
        }

        BufferedRecord bufferedRecord = new BufferedRecord(record);
        buffer.add(bufferedRecord);
        watermark = recordTimestamp - maxOutOfOrdernessMs;

        emitReadyRecords(emitted);

        return emitted;
    }

    private void emitReadyRecords(List<NormalizedRecord> emitted) {
        while (!buffer.isEmpty() && buffer.peek().getTimestamp() <= watermark) {
            BufferedRecord ready = buffer.poll();
            if (ready != null) {
                emitted.add(ready.getRecord());
                lastEmittedTimestamp = Math.max(lastEmittedTimestamp, ready.getTimestamp());
            }
        }
    }

    public List<NormalizedRecord> flush() {
        List<NormalizedRecord> result = new ArrayList<>();
        while (!buffer.isEmpty()) {
            BufferedRecord record = buffer.poll();
            if (record != null) {
                result.add(record.getRecord());
            }
        }
        return result;
    }

    public long getWatermark() {
        return watermark;
    }

    public int getBufferSize() {
        return buffer.size();
    }

    public long getMaxOutOfOrdernessMs() {
        return maxOutOfOrdernessMs;
    }

    private static class BufferedRecord implements Serializable {
        private static final long serialVersionUID = 1L;

        private final NormalizedRecord record;
        private final long arrivalTime;

        BufferedRecord(NormalizedRecord record) {
            this.record = record;
            this.arrivalTime = System.currentTimeMillis();
        }

        NormalizedRecord getRecord() {
            return record;
        }

        long getTimestamp() {
            return record.getTimestamp();
        }

        long getArrivalTime() {
            return arrivalTime;
        }
    }
}
