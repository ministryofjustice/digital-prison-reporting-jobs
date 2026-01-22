package uk.gov.justice.digital.service.metrics;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Interface for services that support batched metric reporting.
 * 'Batched', in this context, means that metrics are stored locally
 * and then flushed to the underlying metrics storage in batches.
 * This is useful to avoid excessive writes to the metrics storage,
 * which can improve performance and reduce costs. For example,
 * we might flush once per micro-batch in a structured streaming job,
 * or once for the whole of a batch job
 */
public interface BatchedMetricReportingService {
    void withBatchedMetrics(Consumer<BatchMetrics> func);
    <T> T withBatchedMetrics(Function<BatchMetrics, T> func);
}
