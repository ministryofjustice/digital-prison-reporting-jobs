package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.OperationalDataStoreException;
import uk.gov.justice.digital.exception.ReconciliationDataSourceException;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResult;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;

import java.util.List;

import static uk.gov.justice.digital.client.s3.S3DataProvider.isPathDoesNotExistException;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class CurrentStateCountService {

    private static final Logger logger = LoggerFactory.getLogger(CurrentStateCountService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;
    private final ReconciliationDataSourceService dataSourceService;
    private final OperationalDataStoreService operationalDataStoreService;

    @Inject
    public CurrentStateCountService(
            JobArguments jobArguments,
            S3DataProvider s3DataProvider,
            ReconciliationDataSourceService reconciliationDataSourceService,
            OperationalDataStoreService operationalDataStoreService
    ) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
        this.dataSourceService = reconciliationDataSourceService;
        this.operationalDataStoreService = operationalDataStoreService;
    }

    /**
     * Retrieve record counts from data stores that contain current state for every SourceReference/table.
     */
    DataReconciliationResult currentStateCounts(SparkSession sparkSession, List<SourceReference> sourceReferences) {
        CurrentStateTotalCounts currentStateCountResults = new CurrentStateTotalCounts();
        sourceReferences.forEach(sourceReference -> {
            CurrentStateTableCount countResults = currentStateCountForTable(sparkSession, sourceReference);
            currentStateCountResults.put(sourceReference.getFullDatahubTableName(), countResults);
        });
        return currentStateCountResults;
    }

    /**
     * Retrieve record counts from data stores that contain current state
     * (the source datastore, structured and curated, and possibly the OperationalDataStore).
     */
    CurrentStateTableCount currentStateCountForTable(SparkSession sparkSession, SourceReference sourceReference) {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        logger.info("Getting current state counts across data stores for table {}.{}", sourceName, tableName);

        long sourceDataStoreCount = getSourceDataStoreCount(tableName);

        String structuredTablePath = tablePath(jobArguments.getStructuredS3Path(), sourceName, tableName);
        long structuredCount = getStructuredCount(sparkSession, structuredTablePath, sourceName, tableName);

        String curatedTablePath = tablePath(jobArguments.getCuratedS3Path(), sourceName, tableName);
        long curatedCount = getCuratedCount(sparkSession, curatedTablePath, sourceName, tableName, structuredTablePath);

        String operationalDataStoreFullTableName = sourceReference.getFullOperationalDataStoreTableNameWithSchema();
        Long operationalDataStoreCount = getOperationalDataStoreCount(sourceReference, operationalDataStoreFullTableName);

        logger.info("Finished current state counts across data stores for table {}.{}", sourceName, tableName);
        double relativeTolerance = jobArguments.getReconciliationChangeDataCountsToleranceRelativePercentage();
        long absoluteTolerance = jobArguments.getReconciliationChangeDataCountsToleranceAbsolute();

        return new CurrentStateTableCount(relativeTolerance, absoluteTolerance, sourceDataStoreCount, structuredCount, curatedCount, operationalDataStoreCount);
    }

    private long getSourceDataStoreCount(String tableName) {
        long sourceDataStoreCount;
        try {
            logger.info("Reading Source Data Store count for table {}", tableName);
            sourceDataStoreCount = dataSourceService.getTableRowCount(tableName);
        } catch (ReconciliationDataSourceException e) {
            logger.warn("Exception when reading source data store count, setting count to zero", e);
            sourceDataStoreCount = 0L;
        }
        return sourceDataStoreCount;
    }

    private long getStructuredCount(SparkSession sparkSession, String structuredTablePath, String sourceName, String tableName) {
        long structuredCount;
        try {
            Dataset<Row> structured = s3DataProvider.getBatchDeltaTableData(sparkSession, structuredTablePath);
            logger.info("Reading Structured count for table {}/{}", sourceName, tableName);
            structuredCount = structured.count();
        } catch (Exception e) {
            if (isPathDoesNotExistException(e)) {
                logger.warn("Structured table does not exist at {} so will set count to zero", structuredTablePath, e);
                structuredCount = 0L;
            } else {
                throw e;
            }
        }
        return structuredCount;
    }

    private long getCuratedCount(SparkSession sparkSession, String curatedTablePath, String sourceName, String tableName, String structuredTablePath) {
        long curatedCount;
        try {
            Dataset<Row> curated = s3DataProvider.getBatchDeltaTableData(sparkSession, curatedTablePath);
            logger.info("Reading Curated count for table {}/{}", sourceName, tableName);
            curatedCount = curated.count();
        } catch (Exception e) {
            if (isPathDoesNotExistException(e)) {
                logger.warn("Structured table does not exist at {} so will set count to zero", structuredTablePath, e);
                curatedCount = 0L;
            } else {
                throw e;
            }
        }
        return curatedCount;
    }

    private @Nullable Long getOperationalDataStoreCount(SourceReference sourceReference, String operationalDataStoreFullTableName) {
        Long operationalDataStoreCount;
        if (operationalDataStoreService.isEnabled() && operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference)) {
            logger.info("Reading Operational DataStore count for managed table {}", operationalDataStoreFullTableName);
            try {
                operationalDataStoreCount = operationalDataStoreService.getTableRowCount(operationalDataStoreFullTableName);
            } catch (OperationalDataStoreException e) {
                operationalDataStoreCount = 0L;
            }
        } else {
            logger.info("Skipping reading Operational DataStore count for table {}", operationalDataStoreFullTableName);
            // Null indicates ODS does not manage this table or functionality is disabled
            operationalDataStoreCount = null;
        }
        return operationalDataStoreCount;
    }
}
