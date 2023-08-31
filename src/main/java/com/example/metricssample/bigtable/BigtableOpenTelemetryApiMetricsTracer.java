package com.example.metricssample.bigtable;

import com.google.api.gax.tracing.OpenTelemetryMetricsTracer;
import com.google.api.gax.tracing.SpanName;
import com.google.cloud.bigtable.data.v2.models.Row;
import io.opentelemetry.api.metrics.Meter;

public class BigtableOpenTelemetryApiMetricsTracer extends OpenTelemetryMetricsTracer {

    public static final String BIGTABLE_ATTEMPT_LATENCY = "cloud.google.com_java_bigtable_attempt_latency";

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
