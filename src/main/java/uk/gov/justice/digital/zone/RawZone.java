package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.col;

@Singleton
public class RawZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    private static final String LOAD = "load";
    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private static final String OPERATION = "operation";
    private static final String PATH = "path";

    private final String rawPath;

    @Inject
    public RawZone(JobParameters jobParameters) {
        this.rawPath = jobParameters.getRawS3Path()
            .orElseThrow(() -> new IllegalStateException("raw s3 path not set - unable to create RawZone instance"));
    }

    @Override
    public void process(Dataset<Row> dataFrame) {

        logger.info("Processing data frame with " + dataFrame.count() + " rows");

        val startTime = System.currentTimeMillis();

        tableLoadEvents(dataFrame).forEach(row -> {
            String rowSource = row.getAs(SOURCE);
            String rowTable = row.getAs(TABLE);
            String rowOperation = row.getAs(OPERATION);

            val tableName = SourceReferenceService.getTable(rowSource, rowTable);
            val sourceName = SourceReferenceService.getTable(rowSource, rowTable);
            val tablePath = getTablePath(rawPath, sourceName, tableName, rowOperation);

            dataFrame
                .filter(col(SOURCE).isin(sourceName).and(col(TABLE).isin(tableName)))
                .drop(SOURCE, TABLE, OPERATION)
                .write()
                .mode(SaveMode.Append)
                .option(PATH, tablePath)
                .format("delta")
                .save();
        });

        logger.info("Processed data frame with {} rows in {}ms",
            dataFrame.count(),
            System.currentTimeMillis() - startTime
        );
    }

    private List<Row> tableLoadEvents(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).isin(LOAD))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

}
