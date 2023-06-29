package uk.gov.justice.digital.zone.curated;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.zone.Zone;

import javax.inject.Inject;

import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

public abstract class CuratedZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(CuratedZone.class);

    private final String curatedPath;
    private final SourceReferenceService sourceReferenceService;
    private final Writer writer;

    @Inject
    public CuratedZone(
            JobArguments jobArguments,
            SourceReferenceService sourceReferenceService,
            Writer writer
    ) {
        this.curatedPath = jobArguments.getCuratedS3Path();
        this.writer = writer;
        this.sourceReferenceService = sourceReferenceService;
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {

        val count = dataFrame.count();

        logger.info("Processing batch with {} records", count);

        val startTime = System.currentTimeMillis();

        String sourceName = table.getAs(SOURCE);
        String tableName = table.getAs(TABLE);

        val sourceReference = sourceReferenceService
                .getSourceReference(sourceName, tableName)
                // This can only happen if the schema disappears after the structured zone has processed the data, so we
                // should never see this in practise. However, if it does happen throwing here will make it clear what
                // has happened.
                .orElseThrow(() -> new IllegalStateException(
                        "Unable to locate source reference data for source: " + sourceName + " table: " + tableName
                ));

        val curatedTablePath = createValidatedPath(
                curatedPath,
                sourceReference.getSource(),
                sourceReference.getTable()
        );

        writer.writeValidRecords(spark, curatedTablePath, sourceReference.getPrimaryKey(), dataFrame);

        logger.info("Processed batch with {} records in {}ms",
                count,
                System.currentTimeMillis() - startTime
        );

        return dataFrame;
    }

}