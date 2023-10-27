package uk.gov.justice.digital.zone.structured;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.writer.structured.StructuredZoneLoadWriter;

import javax.inject.Inject;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.Load;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;

public class StructuredZoneLoadS3 extends StructuredZoneS3 {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZoneLoadS3.class);

    private final ViolationService violationService;

    @Inject
    public StructuredZoneLoadS3(
            JobArguments jobArguments,
            DataStorageService storage,
            ViolationService violationService
    ) {
        this(jobArguments, createWriter(storage), violationService);
    }

    @SuppressWarnings("unused")
    private StructuredZoneLoadS3(
            JobArguments jobArguments,
            Writer writer,
            ViolationService violationService
    ) {
        super(jobArguments, writer);
        this.violationService = violationService;
    }

    private static Writer createWriter(DataStorageService storage) {
        return new StructuredZoneLoadWriter(storage);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        Dataset<Row> result;
        val filteredRecords = dataFrame.filter(col(OPERATION).equalTo(Load.getName()));
        try {
            if (filteredRecords.isEmpty()) {
                result = spark.emptyDataFrame();
            } else {
                String sourceName = sourceReference.getSource();
                String tableName = sourceReference.getTable();

                val startTime = System.currentTimeMillis();

                logger.debug("Processing records for {}/{}", sourceName, tableName);
                result = super.process(spark, filteredRecords, sourceReference);
                logger.debug("Processed batch in {}ms", System.currentTimeMillis() - startTime);

                return result;
            }
        } catch (DataStorageRetriesExhaustedException e) {
            violationService.handleRetriesExhausted(spark, filteredRecords, sourceReference.getSource(), sourceReference.getTable(), e, ViolationService.ZoneName.STRUCTURED_LOAD);
            result = spark.emptyDataFrame();
        }
        return result;
    }

}
