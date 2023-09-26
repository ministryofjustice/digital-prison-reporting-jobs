package uk.gov.justice.digital.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Responsible for processing a batch of data
 */
public interface BatchProcessor {
    void processBatch(Dataset<Row> batch);
}
