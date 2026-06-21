/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import com.amazonaws.services.timestream.TimestreamPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class BatchCoordinator implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(BatchCoordinator.class);

    private final int targetBatchSize;
    private final int maxBatchSize;
    private final int minBatchSize;
    private final long maxBatchLatencyMs;
    private final boolean adaptiveBatching;

    private final AtomicInteger currentBatchSize = new AtomicInteger(0);
    private final AtomicLong batchStartTime = new AtomicLong(0);
    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private final AtomicLong totalLatency = new AtomicLong(0);

    private final List<List<TimestreamPoint>> pendingBatches = new ArrayList<>();

    public BatchCoordinator(int targetBatchSize, int maxBatchSize, int minBatchSize,
                            long maxBatchLatencyMs, boolean adaptiveBatching) {
        this.targetBatchSize = targetBatchSize;
        this.maxBatchSize = maxBatchSize;
        this.minBatchSize = minBatchSize;
        this.maxBatchLatencyMs = maxBatchLatencyMs;
        this.adaptiveBatching = adaptiveBatching;
    }

    public synchronized List<List<TimestreamPoint>> addBatch(List<TimestreamPoint> batch) {
        if (batch == null || batch.isEmpty()) {
            return new ArrayList<>();
        }

        if (batchStartTime.get() == 0) {
            batchStartTime.set(System.currentTimeMillis());
        }

        pendingBatches.add(batch);
        currentBatchSize.addAndGet(batch.size());
        totalRecordsProcessed.addAndGet(batch.size());

        List<List<TimestreamPoint>> readyBatches = new ArrayList<>();

        if (shouldFlush()) {
            readyBatches.addAll(flushInternal());
        }

        return readyBatches;
    }

    public synchronized List<List<TimestreamPoint>> flush() {
        return flushInternal();
    }

    private List<List<TimestreamPoint>> flushInternal() {
        List<List<TimestreamPoint>> result = new ArrayList<>(pendingBatches);
        pendingBatches.clear();

        if (!result.isEmpty()) {
            long now = System.currentTimeMillis();
            long latency = now - batchStartTime.get();
            totalBatches.incrementAndGet();
            totalLatency.addAndGet(latency);
        }

        currentBatchSize.set(0);
        batchStartTime.set(0);

        return result;
    }

    private boolean shouldFlush() {
        int currentSize = currentBatchSize.get();

        if (currentSize >= targetBatchSize) {
            return true;
        }

        long startTime = batchStartTime.get();
        if (startTime > 0) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed >= maxBatchLatencyMs && currentSize >= minBatchSize) {
                return true;
            }
        }

        if (currentSize >= maxBatchSize) {
            return true;
        }

        return false;
    }

    public int getCurrentBatchSize() {
        return currentBatchSize.get();
    }

    public int getPendingBatchCount() {
        return pendingBatches.size();
    }

    public long getTotalRecordsProcessed() {
        return totalRecordsProcessed.get();
    }

    public long getTotalBatches() {
        return totalBatches.get();
    }

    public double getAverageBatchSize() {
        long batches = totalBatches.get();
        if (batches == 0) return 0.0;
        return (double) totalRecordsProcessed.get() / batches;
    }

    public double getAverageLatencyMs() {
        long batches = totalBatches.get();
        if (batches == 0) return 0.0;
        return (double) totalLatency.get() / batches;
    }

    public int getEffectiveBatchSize() {
        if (!adaptiveBatching) {
            return targetBatchSize;
        }

        double avgSize = getAverageBatchSize();
        double avgLatency = getAverageLatencyMs();

        int effectiveSize = targetBatchSize;

        if (avgLatency < maxBatchLatencyMs * 0.5 && avgSize < targetBatchSize * 0.8) {
            effectiveSize = Math.min(targetBatchSize * 2, maxBatchSize);
        } else if (avgLatency > maxBatchLatencyMs * 0.9) {
            effectiveSize = Math.max(targetBatchSize / 2, minBatchSize);
        }

        return effectiveSize;
    }

    public int getTargetBatchSize() {
        return targetBatchSize;
    }

    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    public int getMinBatchSize() {
        return minBatchSize;
    }

    public long getMaxBatchLatencyMs() {
        return maxBatchLatencyMs;
    }

    public boolean isAdaptiveBatching() {
        return adaptiveBatching;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int targetBatchSize = 100;
        private int maxBatchSize = 500;
        private int minBatchSize = 10;
        private long maxBatchLatencyMs = 1000L;
        private boolean adaptiveBatching = false;

        public Builder targetBatchSize(int targetBatchSize) {
            this.targetBatchSize = targetBatchSize;
            return this;
        }

        public Builder maxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Builder minBatchSize(int minBatchSize) {
            this.minBatchSize = minBatchSize;
            return this;
        }

        public Builder maxBatchLatencyMs(long maxBatchLatencyMs) {
            this.maxBatchLatencyMs = maxBatchLatencyMs;
            return this;
        }

        public Builder adaptiveBatching(boolean adaptiveBatching) {
            this.adaptiveBatching = adaptiveBatching;
            return this;
        }

        public BatchCoordinator build() {
            return new BatchCoordinator(targetBatchSize, maxBatchSize, minBatchSize,
                    maxBatchLatencyMs, adaptiveBatching);
        }
    }
}
