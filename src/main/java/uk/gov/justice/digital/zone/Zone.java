package uk.gov.justice.digital.zone;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.service.DataStorageService;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.job.model.Columns.*;

public abstract class Zone extends DataStorageService {

    private static final Logger logger = LoggerFactory.getLogger(Zone.class);

    public abstract Dataset<Row> process(Dataset<Row> dataFrame, Row row);


    // These rows are used to select only those records corresponding to a specific source and table that has loads
    // events in the batch being processed.
    protected List<Row> getTablesWithLoadRecords(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).equalTo("load"))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

    protected void appendDataAndUpdateManifestForTable(Dataset<Row> dataFrame, String tablePath) {
        appendToDeltaLakeTable(dataFrame, tablePath);
        updateDeltaManifestForTable(tablePath);
    }

    protected void appendToDeltaLakeTable(Dataset<Row> dataFrame, String tablePath) {
        logger.info("Appending {} records to deltalake table: {}", dataFrame.count(), tablePath);
        append(tablePath, dataFrame);
        logger.info("Append completed successfully");
    }


    protected Dataset<Row> createEmptyDataFrame(Dataset<Row> dataFrame) {
        return dataFrame.sparkSession().createDataFrame(
                dataFrame.sparkSession().emptyDataFrame().javaRDD(),
                dataFrame.schema()
        );
    }
}
