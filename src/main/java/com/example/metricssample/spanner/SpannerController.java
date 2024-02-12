
package com.example.metricssample.spanner;

import com.google.api.gax.core.GaxProperties;
import com.google.api.gax.tracing.ApiTracerFactory;
import com.google.api.gax.tracing.MetricsTracerFactory;
import com.google.api.gax.tracing.OpentelemetryMetricsRecorder;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.opentelemetry.metric.MetricDescriptorStrategy;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.spi.v1.SpannerRpcViews;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
@RequestMapping(path = "/spanner")
public class SpannerController {
  private Spanner spanner;
  private DatabaseClient dbClient;
  private String instanceId = "myinstance";
  private String databaseId = "mydatabase";
  private String table = "Players";

  private final OpenTelemetry openTelemetry;

  SpannerController(OpenTelemetry openTelemetry) throws Exception{
    this.openTelemetry = openTelemetry;
    // Instantiate the client.
    SpannerOptions options = SpannerOptions.newBuilder().setApiTracerFactory(createOpenTelemetryTracerFactory()).build();

    spanner = options.getService();
    // And then create the Spanner database client.
    String projectId = "spanner-demo-326919";
    dbClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));

    // Register GFELatency and GFE Header Missing Count Views
    SpannerRpcViews.registerGfeLatencyAndHeaderMissingCountViews();
  }

  private static ApiTracerFactory createOpenTelemetryTracerFactory() {

    MetricExporter metricExporter = GoogleCloudMetricExporter.createWithConfiguration(
        MetricConfiguration.builder()
            // Configure the cloud project id.  Note: this is autodiscovered by default.
            .setProjectId("spanner-demo-326919")
            .setPrefix("custom.googleapis.com")
            // Configure a strategy for how/when to configure metric descriptors.
            .setDescriptorStrategy(MetricDescriptorStrategy.SEND_ONCE)
            .build());

    // Periodic Metric Reader configuration
    PeriodicMetricReader metricReader =
        PeriodicMetricReader.builder(metricExporter)
            .setInterval(java.time.Duration.ofSeconds(5))
            .build();

    // OpenTelemetry SDK Configuration
    Resource resource = Resource.builder().build();
    SdkMeterProvider sdkMeterProvider =
        SdkMeterProvider.builder().registerMetricReader(metricReader).setResource(resource).build();

    OpenTelemetry openTelemetry =
        OpenTelemetrySdk.builder().setMeterProvider(sdkMeterProvider).build();

    // Meter Creation
    Meter meter =
        openTelemetry
            .meterBuilder("gax")
            .setInstrumentationVersion(GaxProperties.getGaxVersion())
            .build();

    // OpenTelemetry Metrics Recorder
    OpentelemetryMetricsRecorder otelMetricsRecorder = new OpentelemetryMetricsRecorder(meter);

    // Finally, create the Tracer Factory
    return new MetricsTracerFactory(otelMetricsRecorder);
  }

  @GetMapping(path = "/", produces = "application/json")
  public List<Person> getPerson() {
    List<Person> list = new ArrayList<>();
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .read(
                table,
                KeySet.all(), // Read all rows in a table.
                Arrays.asList("id", "name", "email"))) {
      while (resultSet.next()) {
        Person person = new Person();
        person.setId(resultSet.getString(0));
        person.setName(resultSet.getString(1));
        person.setEmail(resultSet.getString(2));
        list.add(person);
      }
    }
    return list;
  }

  @PostMapping(path = "/", consumes = "application/json", produces = "application/json")
  public Person addPerson(@RequestBody Person p) {
    List<Mutation> mutations =
        Collections.singletonList(
            Mutation.newInsertBuilder(table)
                .set("id")
                .to(UUID.randomUUID().toString())
                .set("name")
                .to(p.getName())
                .set("email")
                .to(p.getEmail())
                .build());
    dbClient.write(mutations);
    return p;
  }
}
