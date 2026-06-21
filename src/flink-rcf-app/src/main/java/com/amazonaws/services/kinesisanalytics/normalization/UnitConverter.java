/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleUnaryOperator;

public class UnitConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final double KELVIN_OFFSET = 273.15;
    private static final double BAR_TO_KPA = 100.0;
    private static final double ATM_TO_KPA = 101.325;
    private static final double PSI_TO_KPA = 6.89476;

    private final Map<String, DoubleUnaryOperator> conversionCache;

    public UnitConverter() {
        this.conversionCache = new HashMap<>();
    }

    public double convert(double value, MeasurementUnit fromUnit, MeasurementUnit toUnit) {
        if (fromUnit == toUnit || fromUnit == MeasurementUnit.UNKNOWN || toUnit == MeasurementUnit.UNKNOWN) {
            return value;
        }

        String cacheKey = fromUnit.name() + "_to_" + toUnit.name();
        DoubleUnaryOperator converter = conversionCache.computeIfAbsent(cacheKey, k -> findConverter(fromUnit, toUnit));

        if (converter != null) {
            return converter.applyAsDouble(value);
        }

        throw new IllegalArgumentException(
                "No conversion available from " + fromUnit.getName() + " to " + toUnit.getName());
    }

    public double convertToBaseUnit(double value, MeasurementUnit unit) {
        return value;
    }

    private DoubleUnaryOperator findConverter(MeasurementUnit from, MeasurementUnit to) {
        if (from == MeasurementUnit.TEMPERATURE_C && to == MeasurementUnit.TEMPERATURE_K) {
            return c -> c + KELVIN_OFFSET;
        }
        if (from == MeasurementUnit.TEMPERATURE_K && to == MeasurementUnit.TEMPERATURE_C) {
            return k -> k - KELVIN_OFFSET;
        }
        if (from == MeasurementUnit.PRESSURE_BAR && to == MeasurementUnit.PRESSURE_KPA) {
            return bar -> bar * BAR_TO_KPA;
        }
        if (from == MeasurementUnit.PRESSURE_KPA && to == MeasurementUnit.PRESSURE_BAR) {
            return kpa -> kpa / BAR_TO_KPA;
        }
        if (from == MeasurementUnit.PERCENT && to == MeasurementUnit.MOL_PERCENT) {
            return p -> p;
        }
        if (from == MeasurementUnit.MOL_PERCENT && to == MeasurementUnit.PERCENT) {
            return mol -> mol;
        }
        if (from == MeasurementUnit.DIMENSIONLESS && to == MeasurementUnit.PERCENT) {
            return d -> d * 100.0;
        }
        if (from == MeasurementUnit.PERCENT && to == MeasurementUnit.DIMENSIONLESS) {
            return p -> p / 100.0;
        }
        return null;
    }

    public boolean canConvert(MeasurementUnit fromUnit, MeasurementUnit toUnit) {
        if (fromUnit == toUnit) {
            return true;
        }
        if (fromUnit == MeasurementUnit.UNKNOWN || toUnit == MeasurementUnit.UNKNOWN) {
            return false;
        }

        String cacheKey = fromUnit.name() + "_to_" + toUnit.name();
        if (conversionCache.containsKey(cacheKey)) {
            return true;
        }

        return findConverter(fromUnit, toUnit) != null;
    }
}
