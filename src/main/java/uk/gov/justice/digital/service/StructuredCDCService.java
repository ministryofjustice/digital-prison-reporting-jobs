package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;

import java.util.Arrays;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;

@Singleton
public class StructuredCDCService {

    private final DataStorageService storage;
    private final String structuredTablePath;

    @Inject
    public StructuredCDCService(
        DataStorageService storage,
        JobArguments jobArguments
    ) {
        this.storage = storage;
        this.structuredTablePath = jobArguments.getStructuredS3Path();
    }

    public void applyUpdates(SparkSession spark, Dataset<Row> latestCDCRecordsByPK, SourceReference sourceReference) throws DataStorageRetriesExhaustedException {
        storage.mergeRecords(spark, structuredTablePath, latestCDCRecordsByPK, sourceReference.getPrimaryKey(), Arrays.asList(OPERATION, TIMESTAMP));
        storage.updateDeltaManifestForTable(spark, structuredTablePath);
    }
}
