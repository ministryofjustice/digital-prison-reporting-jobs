package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.exception.DataStorageException;

public abstract class UnfilteredZone {

    public abstract Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row row) throws DataStorageException;

}
