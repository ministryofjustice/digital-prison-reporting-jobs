package uk.gov.justice.digital.service.metrics;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;

import javax.annotation.PreDestroy;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides a scheduler for flushing metrics to Cloudwatch in a background thread.
 */
@Factory
class MetricSchedulerFactory {

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "metrics-flusher");
        // This thread shouldn't stop the application exiting
        t.setDaemon(true);
        return t;
    });

    @Bean
    @Singleton
    @Named("metricsFlusher")
    ScheduledExecutorService metricsFlusher() {
        return this.executor;
    }

    /**
     * Shutdown the scheduler
     */
    @PreDestroy
    void shutDown() {
        this.executor.shutdown();
    }
}
