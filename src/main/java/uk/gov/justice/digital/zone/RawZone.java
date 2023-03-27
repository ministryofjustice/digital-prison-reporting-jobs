package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.job.model.Columns.*;

@Singleton
public class RawZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

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

        getTablesWithLoadRecords(dataFrame).forEach(row -> {
            String rowSource = row.getAs(SOURCE);
            String rowTable = row.getAs(TABLE);
            String rowOperation = row.getAs(OPERATION);

            val tablePath = SourceReferenceService.getSourceReference(rowSource, rowTable)
                .map(r -> getTablePath(rawS3Path, r, rowOperation))
                // Revert to source and table from row where no match exists in the schema reference service.
                .orElse(getTablePath(rawS3Path, rowSource, rowTable, rowOperation));

            val rowsForTable = dataFrame
                .filter(col(SOURCE).equalTo(rowSource).and(col(TABLE).equalTo(rowTable)))
                .drop(SOURCE, TABLE, OPERATION);

            appendToDeltaLakeTable(rowsForTable, tablePath);
        });

        logger.info("Processed data frame with {} rows in {}ms",
            dataFrame.count(),
            System.currentTimeMillis() - startTime
        );
    }

}
