package uk.gov.justice.digital.job.cdc;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.TableStreamingQueryTimeoutDuringStopException;

import java.util.concurrent.TimeoutException;

import static uk.gov.justice.digital.common.StreamingQuery.getQueryCheckpointPath;
import static uk.gov.justice.digital.common.StreamingQuery.getQueryName;

/**
 * Encapsulates logic for processing a stream of micro-batches of CDC events for a single table.
 * You can test behaviour across multiple batches by testing against this class.
 */
public class TableStreamingQuery {

    private static final Logger logger = LoggerFactory.getLogger(TableStreamingQuery.class);

    private final String inputSourceName;
    private final String inputTableName;
    private final String checkpointLocation;

    private final Dataset<Row> sourceData;
    private final VoidFunction2<Dataset<Row>, Long> batchProcessingFunc;

    private StreamingQuery query;

    public TableStreamingQuery(
            String inputSourceName,
            String inputTableName,
            String checkpointLocation,
            Dataset<Row> sourceData,
            VoidFunction2<Dataset<Row>, Long> batchProcessingFunc) {
        this.inputSourceName = inputSourceName;
        this.inputTableName = inputTableName;
        this.checkpointLocation = checkpointLocation;
        this.sourceData = sourceData;
        this.batchProcessingFunc = batchProcessingFunc;
    }


    public StreamingQuery runQuery() {
        logger.info("Initialising per batch processing for {}/{}", inputSourceName, inputTableName);
        String queryName = getQueryName(inputSourceName, inputTableName);
        String queryCheckpointPath = getQueryCheckpointPath(checkpointLocation, inputSourceName, inputTableName);

        logger.info("Initialising query {} with checkpoint path {}", queryName, queryCheckpointPath);
        try {
            query = sourceData
                    .writeStream()
                    .queryName(queryName)
                    .format("delta")
                    .foreachBatch(batchProcessingFunc)
                    .outputMode("update")
                    .option("checkpointLocation", queryCheckpointPath)
                    .start();
            logger.info("Started query {}", queryName);
            return query;
        } catch (TimeoutException e) {
            logger.error("Encountered TimeoutException when running streaming query start", e);
            throw new RuntimeException(e);
        }
    }

    public void stopQuery() throws TableStreamingQueryTimeoutDuringStopException {
        try {
            query.stop();
        } catch (TimeoutException e) {
            throw new TableStreamingQueryTimeoutDuringStopException(e);
        }
    }
}
