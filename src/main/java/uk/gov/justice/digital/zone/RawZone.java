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
import static uk.gov.justice.digital.job.model.Columns.*;

@Singleton
public class RawZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    // TODO - move these into a shared location
    private static final String LOAD = "load";
    private static final String PATH = "path";

    private final String rawS3Path;

    @Inject
    public RawZone(JobParameters jobParameters) {
        this.rawS3Path = jobParameters.getRawS3Path()
            .orElseThrow(() -> new IllegalStateException("raw s3 path not set - unable to create RawZone instance"));
    }

    @Override
    public void process(Dataset<Row> dataFrame) {

        logger.info("Processing data frame with " + dataFrame.count() + " rows");

        val startTime = System.currentTimeMillis();

        val uniqueLoadEventIdentifiers = uniqueTablesForLoad(dataFrame);

        logger.info("Processing {} unique load events", uniqueLoadEventIdentifiers.size());

        uniqueLoadEventIdentifiers.forEach(row -> {
            String rowSource = row.getAs(SOURCE);
            String rowTable = row.getAs(TABLE);
            String rowOperation = row.getAs(OPERATION);

            // TODO - handle missing schema by writing to source location instead.
            val tableName = SourceReferenceService.getTable(rowSource, rowTable);
            val sourceName = SourceReferenceService.getSource(rowSource, rowTable);
            val tablePath = getTablePath(rawS3Path, sourceName, tableName, rowOperation);

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

    private List<Row> uniqueTablesForLoad(Dataset<Row> dataFrame) {
        return dataFrame
            .filter(col(OPERATION).isin(LOAD))
            .select(TABLE, SOURCE, OPERATION)
            .distinct()
            .collectAsList();
    }

}
