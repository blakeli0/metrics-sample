package com.example.metricssample.bigtable;

import com.google.api.gax.tracing.OpenTelemetryMetricsTracer;
import com.google.api.gax.tracing.SpanName;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.View;

import static com.example.metricssample.bigtable.BigtableOpenTelemetryMetricsFactory.METER_NAME;

public class BigtableOpenTelemetryApiMetricsTracer extends OpenTelemetryMetricsTracer {

    public static final String BIGTABLE_ATTEMPT_LATENCY = "cloud.google.com_java_bigtable_attempt_latency";

    public static final Aggregation AGGREGATION_WITH_MILLIS_HISTOGRAM =
            Aggregation.explicitBucketHistogram(
                    ImmutableList.of(
                            0.0, 0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 13.0,
                            16.0, 20.0, 25.0, 30.0, 40.0, 50.0, 65.0, 80.0, 100.0, 130.0, 160.0, 200.0, 250.0,
                            300.0, 400.0, 500.0, 650.0, 800.0, 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 50000.0,
                            100000.0));

    public static final View OPERATION_LATENCY_VIEW = View.builder()
            .setName("custom.googleapis.com/opentelemetry/cloud.google.com/java/bigtable/operation_latency")
            .setDescription("Operation latency in msecs")
            .setAggregation(AGGREGATION_WITH_MILLIS_HISTOGRAM)
            .build();
    public static final InstrumentSelector OPERATION_LATENCY_INSTRUMENT_SELECTOR = InstrumentSelector.builder()
            .setName("operation_latency")
            .setMeterName(METER_NAME)
            .setType(InstrumentType.HISTOGRAM)
            .setUnit("ms")
            .build();

    public static final View ATTEMPT_LATENCY_VIEW = View.builder()
            .setName("custom.googleapis.com/opentelemetry/cloud.google.com/java/bigtable/attempt_latency")
            .setDescription("Attempt latency in msecs")
            .setAggregation(AGGREGATION_WITH_MILLIS_HISTOGRAM)
            .build();
    public static final InstrumentSelector ATTEMPT_LATENCY_INSTRUMENT_SELECTOR = InstrumentSelector.builder()
            .setName(BIGTABLE_ATTEMPT_LATENCY)
            .setMeterName(METER_NAME)
            .setType(InstrumentType.HISTOGRAM)
            .setUnit("ms")
            .build();

    public BigtableOpenTelemetryApiMetricsTracer(Meter meter, SpanName spanName) {
        super(meter, spanName);

    }
    @Override
    public String attemptLatencyName() {
        //it was cloud.google.com/java/bigtable with OpenCensus, but slashes are not allowed anymore in OpenTelemetry.
        // However, it will be overridden by view name(which allows slashes) if views are configured.
        return BIGTABLE_ATTEMPT_LATENCY;
    }

    @Override
    public void operationSucceeded(Object response) {
        if (response instanceof Row) {
            Row row = (Row) response;
            addOperationLatencyLabels("rowKey", row.getKey().toString());
        }

        super.operationSucceeded(response);
    }

}
