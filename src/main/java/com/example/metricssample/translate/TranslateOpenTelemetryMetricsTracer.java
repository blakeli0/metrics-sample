package com.example.metricssample.translate;

import com.google.api.gax.tracing.MetricsRecorder;
import com.google.api.gax.tracing.MetricsTracer;
import com.google.api.gax.tracing.SpanName;
import io.opentelemetry.api.metrics.Meter;

public class TranslateOpenTelemetryMetricsTracer extends MetricsTracer {

    public TranslateOpenTelemetryMetricsTracer(Meter meter, SpanName spanName) {
        super(spanName, new MetricsRecorder(meter));
    }

}
