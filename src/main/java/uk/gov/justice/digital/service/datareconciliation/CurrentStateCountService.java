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
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;

import java.util.List;

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

    CurrentStateTotalCounts currentStateCounts(SparkSession sparkSession, List<SourceReference> sourceReferences) {
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

        String nomisOracleSourceSchema = jobArguments.getNomisSourceSchemaName();
        String oracleFullTableName = nomisOracleSourceSchema + "." + tableName.toUpperCase();
        String operationalDataStoreFullTableName = sourceReference.getFullOperationalDataStoreTableNameWithSchema();

        String structuredTablePath = tablePath(jobArguments.getStructuredS3Path(), sourceName, tableName);
        String curatedTablePath = tablePath(jobArguments.getCuratedS3Path(), sourceName, tableName);
        Dataset<Row> structured = s3DataProvider.getBatchSourceData(sparkSession, structuredTablePath);
        Dataset<Row> curated = s3DataProvider.getBatchSourceData(sparkSession, curatedTablePath);


        logger.info("Reading Nomis count for table {}", oracleFullTableName);
        long nomisCount = nomisDataAccessService.getTableRowCount(oracleFullTableName);
        logger.info("Reading Structured count for table {}/{}", sourceName, tableName);
        long structuredCount = structured.count();
        logger.info("Reading Curated count for table {}/{}", sourceName, tableName);
        long curatedCount = curated.count();

        CurrentStateTableCount result;
        if (operationalDataStoreService.isEnabled() && operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference)) {
            logger.info("Reading Operational DataStore count for managed table {}", operationalDataStoreFullTableName);
            long operationalDataStoreCount = operationalDataStoreService.getTableRowCount(operationalDataStoreFullTableName);
            result = new CurrentStateTableCount(nomisCount, structuredCount, curatedCount, operationalDataStoreCount);
        } else {
            logger.info("Skipping reading Operational DataStore count for table {}", operationalDataStoreFullTableName);
            result = new CurrentStateTableCount(nomisCount, structuredCount, curatedCount);
        }

        logger.info("Finished current state counts across data stores for table {}.{}", sourceName, tableName);

        return result;
    }
}
