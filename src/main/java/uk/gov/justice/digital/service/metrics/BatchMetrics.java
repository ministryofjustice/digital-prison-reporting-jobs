package uk.gov.justice.digital.service.metrics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

/**
 * Responsible for publishing metrics related to Glue job batches or micro batches.
 * Methods prefixed with 'buffer' are used to buffer specific metrics locally.
 * It is not safe to retain a reference to this object after a batch finishes, and
 * you must call 'close' to flush all locally buffered metrics to the underlying
 * metrics storage at the end of a batch.
 */
public interface BatchMetrics extends AutoCloseable {
    void bufferViolationCount(long count);
    void bufferDataReconciliationResults(DataReconciliationResults dataReconciliationResults);
    void bufferStreamingThroughputInput(Dataset<Row> inputDf);
    void bufferStreamingThroughputWrittenToStructured(Dataset<Row> structuredDf);
    void bufferStreamingThroughputWrittenToCurated(Dataset<Row> curatedDf);
    void bufferStreamingMicroBatchTimeTaken(long timeTakenMs);

    /**
     * Flush all buffered metrics to the underlying metrics storage.
     * If you do not call this method, then metrics will not be written to long-term storage.
     */
    @Override
    void close();
}
