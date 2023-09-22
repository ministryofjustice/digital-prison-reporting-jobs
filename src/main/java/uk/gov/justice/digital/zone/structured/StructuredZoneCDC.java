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
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.writer.Writer;
import uk.gov.justice.digital.writer.structured.StructuredZoneCdcWriter;

import javax.inject.Inject;

import static org.apache.spark.sql.functions.col;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.cdcOperations;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.*;

public class StructuredZoneCDC extends StructuredZone {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZoneCDC.class);


    @Inject
    public StructuredZoneCDC(
            JobArguments jobArguments,
            DataStorageService storage
    ) {
        this(jobArguments, createWriter(storage));
    }

    @SuppressWarnings("unused")
    private StructuredZoneCDC(
            JobArguments jobArguments,
            Writer writer
    ) {
        super(jobArguments, writer);
    }

    private static Writer createWriter(DataStorageService storage) {
        return new StructuredZoneCdcWriter(storage);
    }

    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) throws DataStorageException {
        val filteredRecords = dataFrame.filter(col(OPERATION).isin(cdcOperations));

        if (filteredRecords.isEmpty()) {
            return spark.emptyDataFrame();
        } else {
            String sourceName = sourceReference.getSource();
            String tableName = sourceReference.getTable();

            val startTime = System.currentTimeMillis();

            logger.debug("Processing records for {}/{}", sourceName, tableName);
            val result = super.process(spark, filteredRecords, sourceReference);
            logger.debug("Processed batch in {}ms", System.currentTimeMillis() - startTime);

            return result;
        }
    }

}
