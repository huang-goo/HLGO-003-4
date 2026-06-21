/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import java.io.Serializable;

public class OutlierClipper implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum ClipStrategy {
        CLAMP_TO_BOUNDARY,
        REPLACE_WITH_DEFAULT,
        MARK_AS_INVALID,
        CLAMP_TO_TYPICAL
    }

    private final ClipStrategy strategy;
    private final boolean useTypicalRange;
    private final double stdDevMultiplier;
    private final boolean enableStatisticalClipping;

    private final java.util.Map<String, RunningStats> statsMap;

    public OutlierClipper() {
        this(ClipStrategy.CLAMP_TO_BOUNDARY, false, 3.0, false);
    }

    public OutlierClipper(ClipStrategy strategy, boolean useTypicalRange,
                          double stdDevMultiplier, boolean enableStatisticalClipping) {
        this.strategy = strategy;
        this.useTypicalRange = useTypicalRange;
        this.stdDevMultiplier = stdDevMultiplier;
        this.enableStatisticalClipping = enableStatisticalClipping;
        this.statsMap = new java.util.concurrent.ConcurrentHashMap<>();
    }

    public ClipResult clip(String fieldName, double value, XmeasFieldMetadata metadata) {
        boolean clipped = false;
        double clippedValue = value;
        String clipReason = null;

        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return handleInvalidValue(value, metadata);
        }

        double lowerBound = useTypicalRange ? metadata.getTypicalMin() : metadata.getMinValidValue();
        double upperBound = useTypicalRange ? metadata.getTypicalMax() : metadata.getMaxValidValue();

        if (enableStatisticalClipping && statsMap.containsKey(fieldName)) {
            RunningStats stats = statsMap.get(fieldName);
            if (stats.getCount() > 30) {
                double mean = stats.getMean();
                double stdDev = stats.getStdDev();
                double statLower = mean - stdDevMultiplier * stdDev;
                double statUpper = mean + stdDevMultiplier * stdDev;

                lowerBound = Math.max(lowerBound, statLower);
                upperBound = Math.min(upperBound, statUpper);
            }
        }

        if (value < lowerBound) {
            clipped = true;
            clipReason = "below_lower_bound";
            switch (strategy) {
                case CLAMP_TO_BOUNDARY:
                case CLAMP_TO_TYPICAL:
                    clippedValue = lowerBound;
                    break;
                case REPLACE_WITH_DEFAULT:
                    clippedValue = metadata.getDefaultValue();
                    break;
                case MARK_AS_INVALID:
                    clippedValue = Double.NaN;
                    break;
            }
        } else if (value > upperBound) {
            clipped = true;
            clipReason = "above_upper_bound";
            switch (strategy) {
                case CLAMP_TO_BOUNDARY:
                case CLAMP_TO_TYPICAL:
                    clippedValue = upperBound;
                    break;
                case REPLACE_WITH_DEFAULT:
                    clippedValue = metadata.getDefaultValue();
                    break;
                case MARK_AS_INVALID:
                    clippedValue = Double.NaN;
                    break;
            }
        }

        if (enableStatisticalClipping && !clipped) {
            updateStats(fieldName, value);
        }

        return new ClipResult(clippedValue, clipped, clipReason);
    }

    private ClipResult handleInvalidValue(double value, XmeasFieldMetadata metadata) {
        double result;
        switch (strategy) {
            case REPLACE_WITH_DEFAULT:
                result = metadata.getDefaultValue();
                break;
            case CLAMP_TO_BOUNDARY:
            case CLAMP_TO_TYPICAL:
                result = useTypicalRange ?
                        (Double.isNaN(value) ? metadata.getTypicalMin() :
                                (value < 0 ? metadata.getTypicalMin() : metadata.getTypicalMax())) :
                        (Double.isNaN(value) ? metadata.getMinValidValue() :
                                (value < 0 ? metadata.getMinValidValue() : metadata.getMaxValidValue()));
                break;
            case MARK_AS_INVALID:
            default:
                result = Double.NaN;
                break;
        }
        return new ClipResult(result, true, "invalid_input");
    }

    private void updateStats(String fieldName, double value) {
        statsMap.computeIfAbsent(fieldName, k -> new RunningStats()).add(value);
    }

    public void resetStats(String fieldName) {
        statsMap.remove(fieldName);
    }

    public void resetAllStats() {
        statsMap.clear();
    }

    public static class ClipResult implements Serializable {
        private static final long serialVersionUID = 1L;

        private final double value;
        private final boolean clipped;
        private final String reason;

        public ClipResult(double value, boolean clipped, String reason) {
            this.value = value;
            this.clipped = clipped;
            this.reason = reason;
        }

        public double getValue() {
            return value;
        }

        public boolean isClipped() {
            return clipped;
        }

        public String getReason() {
            return reason;
        }
    }

    private static class RunningStats implements Serializable {
        private static final long serialVersionUID = 1L;

        private long count = 0;
        private double mean = 0.0;
        private double m2 = 0.0;

        synchronized void add(double value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            m2 += delta * delta2;
        }

        long getCount() {
            return count;
        }

        double getMean() {
            return mean;
        }

        double getStdDev() {
            if (count < 2) return 0.0;
            return Math.sqrt(m2 / (count - 1));
        }
    }
}
