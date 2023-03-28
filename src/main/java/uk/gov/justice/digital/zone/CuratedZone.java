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

import static uk.gov.justice.digital.job.model.Columns.SOURCE;
import static uk.gov.justice.digital.job.model.Columns.TABLE;

@Singleton
public class CuratedZone extends Zone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZone.class);

    private final String curatedPath;

    @Inject
    public CuratedZone(JobParameters jobParameters) {
        this.curatedPath = jobParameters.getCuratedS3Path()
                .orElseThrow(() -> new IllegalStateException(
                        "structured s3 path not set - unable to create StructuredZone instance"
                ));
    }

    @Override
    public Dataset<Row> process(Dataset<Row> dataFrame, Row row) {
        val curatedRecordsCount = dataFrame.count();
        logger.info("Processing batch with {} records", curatedRecordsCount);
        if (curatedRecordsCount > 0) {
            val startTime = System.currentTimeMillis();

            val sourceReference = SourceReferenceService.getSourceReference(row.getAs(SOURCE), row.getAs(TABLE));
            val curatedTablePath = getTablePath(curatedPath, sourceReference.get());

            appendToDeltaLakeTable(dataFrame, curatedTablePath);

            logger.info("Processed data frame with {} rows in {}ms",
                    curatedRecordsCount,
                    System.currentTimeMillis() - startTime
            );
            return dataFrame;
        } else {
            return createEmptyDataFrame(dataFrame);
        }
    }


}
