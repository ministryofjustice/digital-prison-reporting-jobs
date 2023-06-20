package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.exception.DataStorageException;

public abstract class FilteredZone {

    public abstract Dataset<Row> process(
            SparkSession spark,
            Dataset<Row> dataFrame,
            Row table,
            Boolean isCDC
    ) throws DataStorageException;

    protected Dataset<Row> createEmptyDataFrame(Dataset<Row> dataFrame) {
        return dataFrame.sparkSession().createDataFrame(
                dataFrame.sparkSession().emptyDataFrame().javaRDD(),
                dataFrame.schema()
        );
    }
}
