package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.metrics.BatchMetrics;

public interface Zone {
    Dataset<Row> process(SparkSession spark, BatchMetrics batchMetrics, Dataset<Row> dataFrame, SourceReference sourceReference);
}
