package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.NomisDataAccessService;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateCountTableResult;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class CurrentStateCountService {

    private static final Logger logger = LoggerFactory.getLogger(CurrentStateCountService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;
    private final NomisDataAccessService nomisDataAccessService;
    private final OperationalDataStoreService operationalDataStoreService;

    @Inject
    public CurrentStateCountService(
            JobArguments jobArguments,
            S3DataProvider s3DataProvider,
            NomisDataAccessService nomisDataAccessService,
            OperationalDataStoreService operationalDataStoreService
    ) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
        this.nomisDataAccessService = nomisDataAccessService;
        this.operationalDataStoreService = operationalDataStoreService;
    }

    /**
     * Retrieve record counts from data stores that contain current state
     * (the source datastore, structured and curated, and possibly the OperationalDataStore).
     */
    CurrentStateCountTableResult currentStateCounts(SparkSession sparkSession, SourceReference sourceReference) {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        logger.info("Getting current state counts across data stores for table {}.{}", sourceName, tableName);

        String nomisOracleSourceSchema = jobArguments.getNomisSourceSchemaName();
        String oracleFullTableName = nomisOracleSourceSchema + "." + tableName.toUpperCase();
        String operationalDataStoreFullTableName = sourceReference.getFullOperationalDataStoreTableNameWithSchema();

        String structuredPath = tablePath(jobArguments.getStructuredS3Path(), sourceName, tableName);
        String curatedPath = tablePath(jobArguments.getCuratedS3Path(), sourceName, tableName);
        Dataset<Row> structured = s3DataProvider.getBatchSourceData(sparkSession, structuredPath);
        Dataset<Row> curated = s3DataProvider.getBatchSourceData(sparkSession, curatedPath);


        logger.info("Reading Nomis count for table {}", oracleFullTableName);
        long nomisCount = nomisDataAccessService.getTableRowCount(oracleFullTableName);
        logger.info("Reading Structured count for table {}/{}", sourceName, tableName);
        long structuredCount = structured.count();
        logger.info("Reading Curated count for table {}/{}", sourceName, tableName);
        long curatedCount = curated.count();

        CurrentStateCountTableResult result;
        if (operationalDataStoreService.isEnabled() && operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference)) {
            logger.info("Reading Operational DataStore count for managed table {}", operationalDataStoreFullTableName);
            long operationalDataStoreCount = operationalDataStoreService.getTableRowCount(operationalDataStoreFullTableName);
            result = new CurrentStateCountTableResult(nomisCount, structuredCount, curatedCount, operationalDataStoreCount);
        } else {
            logger.info("Skipping reading Operational DataStore count for table {}", operationalDataStoreFullTableName);
            result = new CurrentStateCountTableResult(nomisCount, structuredCount, curatedCount);
        }

        logger.info("Finished current state counts across data stores for table {}.{}", sourceName, tableName);

        return result;
    }
}
