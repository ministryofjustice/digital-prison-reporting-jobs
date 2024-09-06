package uk.gov.justice.digital.service.datareconciliation;

import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.service.NomisDataAccessService;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.List;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;


/**
 * Entry point for all data reconciliation in the DataHub.
 */
@Singleton
public class DataReconciliationService {

    private static final Logger logger = LoggerFactory.getLogger(DataReconciliationService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;
    private final NomisDataAccessService nomisDataAccessService;
    private final ConfigService configService;
    private final SourceReferenceService sourceReferenceService;

    @Inject
    public DataReconciliationService(
            JobArguments jobArguments,
            S3DataProvider s3DataProvider,
            NomisDataAccessService nomisDataAccessService,
            ConfigService configService,
            SourceReferenceService sourceReferenceService
    ) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
        this.nomisDataAccessService = nomisDataAccessService;
        this.configService = configService;
        this.sourceReferenceService = sourceReferenceService;
    }

    public CurrentStateTotalCountResults reconcileDataOrThrow(SparkSession sparkSession) {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(jobArguments.getConfigKey());
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        CurrentStateTotalCountResults results = new CurrentStateTotalCountResults();
        allSourceReferences.forEach(sourceReference -> {
            results.put(sourceReference, currentStateCounts(sparkSession, sourceReference));
        });
        return results;
    }

    /*
     * Retrieve record counts from data stores that contain current state
     * (the source datastore, structured and curated, and possibly the OperationalDataStore).
     */
    private CurrentStateCountTableResult currentStateCounts(SparkSession sparkSession, SourceReference sourceReference) {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();

        String nomisOracleSourceSchema = jobArguments.getNomisSourceSchemaName();
        String oracleFullTableName = nomisOracleSourceSchema + "." + tableName.toUpperCase();

        String structuredPath = tablePath(jobArguments.getStructuredS3Path(), sourceName, tableName);
        String curatedPath = tablePath(jobArguments.getCuratedS3Path(), sourceName, tableName);
        Dataset<Row> structured = s3DataProvider.getBatchSourceData(sparkSession, structuredPath);
        Dataset<Row> curated = s3DataProvider.getBatchSourceData(sparkSession, curatedPath);


        logger.info("Reading Nomis count");
        long nomisCount = nomisDataAccessService.getTableCount(oracleFullTableName);
        logger.info("Reading Structured count");
        long structuredCount = structured.count();
        logger.info("Reading Curated count");
        long curatedCount = curated.count();

        return new CurrentStateCountTableResult(nomisCount, structuredCount, curatedCount);
    }
}

