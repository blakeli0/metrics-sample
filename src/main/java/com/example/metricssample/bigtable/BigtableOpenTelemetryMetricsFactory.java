package com.example.metricssample.bigtable;

import com.google.api.gax.tracing.ApiTracer;
import com.google.api.gax.tracing.OpenTelemetryMetricsFactory;
import com.google.api.gax.tracing.SpanName;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.MeterProvider;

public class BigtableOpenTelemetryMetricsFactory extends OpenTelemetryMetricsFactory {

    public static final String METER_NAME = "java-bigtable";

    private OpenTelemetry openTelemetry;

    public BigtableOpenTelemetryMetricsFactory(OpenTelemetry openTelemetry) {
        super(openTelemetry, METER_NAME, "2.31.1");
        MeterProvider meterProvider = openTelemetry.getMeterProvider();

    }

    @Override
    public ApiTracer newTracer(ApiTracer parent, SpanName spanName, OperationType operationType) {
        return new BigtableOpenTelemetryApiMetricsTracer(meter, spanName);
    }

}
