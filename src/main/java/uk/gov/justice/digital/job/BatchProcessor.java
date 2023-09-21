package uk.gov.justice.digital.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface BatchProcessor {

    void processBatch(Dataset<Row> batch);
}
