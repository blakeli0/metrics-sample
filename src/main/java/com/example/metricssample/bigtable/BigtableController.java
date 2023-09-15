package com.example.metricssample.bigtable;

import com.example.metricssample.common.ProjectConfigs;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.tracing.OpenTelemetryMetricsFactory;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.stub.metrics.RpcMeasureConstants;
import com.google.cloud.opentelemetry.metric.GoogleCloudMetricExporter;
import com.google.cloud.opentelemetry.metric.MetricConfiguration;
import com.google.cloud.opentelemetry.metric.MetricDescriptorStrategy;
import com.google.common.collect.ImmutableList;
import io.opencensus.contrib.grpc.metrics.RpcViews;
import io.opencensus.stats.Measure;
import io.opencensus.stats.Stats;
import io.opencensus.tags.TagKey;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.Aggregation;
import io.opentelemetry.sdk.metrics.InstrumentSelector;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.View;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;

import static com.example.metricssample.bigtable.BigtableOpenTelemetryApiMetricsTracer.BIGTABLE_ATTEMPT_LATENCY;
import static com.example.metricssample.bigtable.BigtableOpenTelemetryApiMetricsTracer.ATTEMPT_LATENCY_INSTRUMENT_SELECTOR;
import static com.example.metricssample.bigtable.BigtableOpenTelemetryApiMetricsTracer.OPERATION_LATENCY_INSTRUMENT_SELECTOR;
import static com.example.metricssample.bigtable.BigtableOpenTelemetryApiMetricsTracer.OPERATION_LATENCY_VIEW;
import static com.example.metricssample.bigtable.BigtableOpenTelemetryApiMetricsTracer.ATTEMPT_LATENCY_VIEW;
import static com.example.metricssample.bigtable.BigtableOpenTelemetryMetricsFactory.METER_NAME;

@RestController
@RequestMapping(path = "/bigtable")
public class BigtableController {
    private BigtableDataClient dataClient;
    private BigtableTableAdminClient adminClient;
    private static final String tableId = "test-metrics-table";
    private static final String COLUMN_FAMILY = "cf1";
    private static final String COLUMN_QUALIFIER_GREETING = "greeting";
    private static final String COLUMN_QUALIFIER_NAME = "name";
    private static final String ROW_KEY_PREFIX = "rowKey";

    private OpenTelemetry openTelemetry;
    private ProjectConfigs projectConfigs;
    static final TagKey BIGTABLE_OP = TagKey.create("bigtable_op");
    static final TagKey BIGTABLE_STATUS = TagKey.create("bigtable_status");
    public BigtableController(OpenTelemetry openTelemetry, ProjectConfigs projectConfigs) throws Exception {
        this.openTelemetry = openTelemetry;
        this.projectConfigs = projectConfigs;
        String instanceId = "test-routing-headers";
        //Register OpenCensus views for gRPC metrics
        RpcViews.registerAllViews();

        OpenTelemetryMetricsFactory openTelemetryTracerFactory = createOpenTelemetryTracerFactory();
        BigtableDataSettings.Builder builder = BigtableDataSettings.newBuilder()
                .setProjectId(projectConfigs.getProjectId())
                .setInstanceId(instanceId);
        ChannelPoolSettings channelPoolSettings = ChannelPoolSettings.builder()
                .setInitialChannelCount(2)
                .setMinChannelCount(2)
                .setMaxChannelCount(10)
                .build();
        InstantiatingGrpcChannelProvider transportChannelProvider = InstantiatingGrpcChannelProvider.newBuilder().setChannelPoolSettings(channelPoolSettings).build();
        builder.stubSettings().setTracerFactory(openTelemetryTracerFactory).setTransportChannelProvider(transportChannelProvider);

        BigtableDataSettings.enableBuiltinMetrics();
        BigtableDataSettings.enableGfeOpenCensusStats();
        BigtableDataSettings.enableOpenCensusStats();
        Measure.MeasureLong bigtableAttemptLatencyMeasure = Measure.MeasureLong.create("cloud.google.com/java/bigtable/attempt_latency", "Duration of an individual operation attempt", "ms");

        io.opencensus.stats.View attemptCountView = io.opencensus.stats.View.create(io.opencensus.stats.View.Name
                        .create("cloud.google.com/java/bigtable/attempt_count"),
                "Attempt count",
                bigtableAttemptLatencyMeasure,
                io.opencensus.stats.Aggregation.Count.create(),
                ImmutableList.of(RpcMeasureConstants.BIGTABLE_PROJECT_ID, RpcMeasureConstants.BIGTABLE_INSTANCE_ID, RpcMeasureConstants.BIGTABLE_APP_PROFILE_ID, BIGTABLE_OP, BIGTABLE_STATUS));
        Stats.getViewManager().registerView(attemptCountView);

        dataClient = BigtableDataClient.create(builder.build());
        BigtableTableAdminSettings adminSettings =
                BigtableTableAdminSettings.newBuilder()
                        .setProjectId(projectConfigs.getProjectId())
                        .setInstanceId(instanceId)
                        .build();
        adminClient = BigtableTableAdminClient.create(adminSettings);
        if (!adminClient.exists(tableId)) {
            adminClient.createTable(CreateTableRequest.of(tableId).addFamily("cf1"));
            writeToTable();
        }
    }

    private OpenTelemetryMetricsFactory createOpenTelemetryTracerFactory() {
        //Default resource is "Generic Task"
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, "bigtable")));

        MetricExporter cloudMonitoringExporter = GoogleCloudMetricExporter.createWithConfiguration(
                MetricConfiguration.builder()
                        // Configure the cloud project id.  Note: this is autodiscovered by default.
                        .setProjectId(projectConfigs.getProjectId())
                        .setPrefix("custom.googleapis.com")
                        // Configure a strategy for how/when to configure metric descriptors.
                        .setDescriptorStrategy(MetricDescriptorStrategy.SEND_ONCE)
                        .build());

        View attemptCountView = View.builder()
                .setName("custom.googleapis.com/opentelemetry/cloud.google.com/java/bigtable/attempt_count")
                .setDescription("Attempt count")
                .setAggregation(Aggregation.sum())
                .build();
        InstrumentSelector attemptCountSelector = InstrumentSelector.builder()
                .setName(BIGTABLE_ATTEMPT_LATENCY)
                .setMeterName(METER_NAME)
                .setType(InstrumentType.HISTOGRAM)
                .build();

        PrometheusHttpServer prometheusReader = PrometheusHttpServer.builder().setPort(9091).build();
        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
                .registerMetricReader(PeriodicMetricReader.builder(cloudMonitoringExporter).setInterval(Duration.ofSeconds(20)).build())
                .registerMetricReader(prometheusReader)
                .setResource(resource)
                .registerView(ATTEMPT_LATENCY_INSTRUMENT_SELECTOR, ATTEMPT_LATENCY_VIEW)
                .registerView(OPERATION_LATENCY_INSTRUMENT_SELECTOR, OPERATION_LATENCY_VIEW)
                .registerView(attemptCountSelector, attemptCountView)
                .build();

        OpenTelemetry openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(sdkMeterProvider)
                .build();

        return new BigtableOpenTelemetryMetricsFactory(openTelemetry);
    }

    @GetMapping(path = "/", produces = "application/json")
    public String getARow() throws Exception {
        Row row = dataClient.readRow(tableId, "rowKey0");
        System.out.println("Received row: " + row.toString());
        return row.toString();
    }

    @GetMapping(path = "/rows", produces = "application/json")
    public String getRows() throws Exception {
        Iterable<Row> rows = dataClient.readRows(Query.create(tableId).limit(10));
        rows.forEach(row -> System.out.println("Received row: " + row.toString()));
        return rows.toString();
    }

    public void writeToTable() {
        try {
            System.out.println("\nWriting some greetings to the table");
            String[] names = {"World", "Bigtable", "Java"};
            for (int i = 0; i < names.length; i++) {
                String greeting = "Hello " + names[i] + "!";
                RowMutation rowMutation =
                        RowMutation.create(tableId, ROW_KEY_PREFIX + i)
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_NAME, names[i])
                                .setCell(COLUMN_FAMILY, COLUMN_QUALIFIER_GREETING, greeting);
                dataClient.mutateRow(rowMutation);
                System.out.println(greeting);
            }
        } catch (NotFoundException e) {
            System.err.println("Failed to write to non-existent table: " + e.getMessage());
        }
    }
}
