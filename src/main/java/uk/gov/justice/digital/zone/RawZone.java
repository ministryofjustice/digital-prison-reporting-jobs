package uk.gov.justice.digital.zone;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.Converter;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.util.Optional;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.*;

@Singleton
public class RawZone extends Zone {

    public static final String PRIMARY_KEY_NAME = "id";
    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    private final String rawS3Path;
    private final DataStorageService storage;

    @Inject
    public RawZone(JobArguments jobArguments, DataStorageService storage) {
        this.rawS3Path = jobArguments.getRawS3Path();
        this.storage = storage;
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, Row table) throws DataStorageException {

        final long count = dataFrame.count(); // this is expensive. Requires a shuffle.

        logger.info("Processing batch with {} records",count);

        val startTime = System.currentTimeMillis();

        String rowSource = table.getAs(SOURCE);
        String rowTable = table.getAs(TABLE);
        Optional<Converter.Operation> rowOperation = Converter.Operation.getOperation(table.getAs(OPERATION));

        if(rowOperation.isPresent()) {

            val tablePath = SourceReferenceService.getSourceReference(rowSource, rowTable)
                    .map(r -> createValidatedPath(rawS3Path, r.getSource(), r.getTable(), rowOperation.get().getName()))
                    // Revert to source and table from row where no match exists in the schema reference service.
                    .orElse(createValidatedPath(rawS3Path, rowSource, rowTable, rowOperation.get().getName()));


            logger.info("AppendDistinct {} records to deltalake table: {}", dataFrame.count(), tablePath);
            // this is the format that raw takes
            val dataFrameToWrite = dataFrame.select(
                    concat(col(KEY), lit(":"), col(TIMESTAMP), lit(":"), col(OPERATION)).as(PRIMARY_KEY_NAME),
                    col(TIMESTAMP), col(KEY), col(SOURCE), col(TABLE), col(OPERATION), col(CONVERTER), col(RAW));

            storage.appendDistinct(tablePath, dataFrameToWrite, PRIMARY_KEY_NAME);

            logger.info("Append completed successfully");
            storage.updateDeltaManifestForTable(spark, tablePath);
        }

        logger.info("Processed batch with {} records in {}ms",
                count,
                System.currentTimeMillis() - startTime
        );

        return dataFrame;
    }


}
