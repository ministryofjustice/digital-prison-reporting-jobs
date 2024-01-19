package uk.gov.justice.digital.job;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleTimer;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Stream {

    private static final Random r = new Random();

    public static void main(String[] args) {
        // Show show a streaming app could push metrics to Prometheus pushgateway on each batch

        String jobName = "testStreamingJob";
        String serverUrl = "localhost:9191";
        PushGateway pg = new PushGateway(serverUrl);
        CollectorRegistry registry = new CollectorRegistry();

        // Counters only go up (or reset to zero on app restart)
        Counter counter = Counter.build()
                .name("recordCount")
                .help("The record count")
                .labelNames("recordType")
                .register(registry);

        // Gauges can go up or down
        Gauge gauge = Gauge.build()
                .name("myGauge")
                .help("A gauge example")
                .register(registry);

        // When pushing metrics we can use a gauge as a timer since we don't miss values due to Prometheus scrape intervals
        Gauge gaugeTimer = Gauge.build()
                .name("gaugeTimer")
                .help("A gauge example used as a timer")
                .labelNames("zone")
                .register(registry);



        // Histograms sample observations, such as request times, in to a distribution.
        // Say we don't want to report every observation but just report bucketed summary metrics.
        // E.g. the histogram buckets might be millisecond response time ranges
        Histogram hist = Histogram.build()
                .name("myHistogram")
                .help("histogram bucketing random data")
                .buckets(2, 5, 10, 20, 50, 90)
                .labelNames("zone")
                .register(registry);

        // Summaries sample observations, such as request times, in to a distribution.
        // Maintain quantiles rather than buckets
        Summary summary = Summary.build()
                .name("mySummary")
                .help("Summary of timings")
                .quantile(0.5, 0.01)
                .quantile(0.95, 0.005)
                .quantile(0.99, 0.005)
                .labelNames("zone")
                .register();

        String zoneName = "structured";
        for (int i = 0; i < 10; i++) {
            try {
                SimpleTimer timer = new SimpleTimer();
                TimeUnit.SECONDS.sleep(5);
                counter.labels("insert").inc();
                gauge.set(i);
                hist.labels(zoneName).observe(randomValueInRange(1, 100));
                double timeSpent = timer.elapsedSeconds();
                summary.labels(zoneName).observe(timeSpent);
                gaugeTimer.labels(zoneName).set(timeSpent);
                pg.push(registry, jobName);
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException("Failed to push metrics", e);
            }

        }
    }

    private static double randomValueInRange(double min, double max) {
        return min + (max - min) * r.nextDouble();
    }
}
