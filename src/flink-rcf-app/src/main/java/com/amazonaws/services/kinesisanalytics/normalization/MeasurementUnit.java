/*Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
SPDX-License-Identifier: MIT-0 */

package com.amazonaws.services.kinesisanalytics.normalization;

public enum MeasurementUnit {
    PERCENT("percent", "%"),
    MOL_PERCENT("mol_percent", "mol%"),
    PRESSURE_KPA("pressure_kpa", "kPa"),
    PRESSURE_BAR("pressure_bar", "bar"),
    TEMPERATURE_C("temperature_c", "°C"),
    TEMPERATURE_K("temperature_k", "K"),
    FLOW_RATE_KGH("flow_rate_kgh", "kg/h"),
    FLOW_RATE_M3H("flow_rate_m3h", "m³/h"),
    DIMENSIONLESS("dimensionless", ""),
    UNKNOWN("unknown", "");

    private final String name;
    private final String symbol;

    MeasurementUnit(String name, String symbol) {
        this.name = name;
        this.symbol = symbol;
    }

    public String getName() {
        return name;
    }

    public String getSymbol() {
        return symbol;
    }
}
