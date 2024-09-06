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
import uk.gov.justice.digital.client.oracle.NomisDataProvider;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.DataReconciliationFailureException;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.List;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;


@Singleton
public class DataReconciliationService {

    private static final Logger logger = LoggerFactory.getLogger(DataReconciliationService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;
    private final NomisDataProvider nomisDataProvider;
    private final ConfigService configService;
    private final SourceReferenceService sourceReferenceService;

    @Inject
    public DataReconciliationService(
            JobArguments jobArguments,
            S3DataProvider s3DataProvider,
            NomisDataProvider nomisDataProvider,
            ConfigService configService,
            SourceReferenceService sourceReferenceService
    ) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
        this.nomisDataProvider = nomisDataProvider;
        this.configService = configService;
        this.sourceReferenceService = sourceReferenceService;
    }

    /**
     * Retrieve record counts from data stores that contain current state
     * (the source datastore, structured and curated, and possibly the OperationalDataStore).
     */
    public CurrentStateCountTableResult currentStateCounts(SparkSession sparkSession, SourceReference sourceReference) {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();

        String nomisOracleSourceSchema = jobArguments.getNomisSourceSchemaName();
        String oracleFullTableName = nomisOracleSourceSchema + "." + tableName.toUpperCase();

        String structuredPath = tablePath(jobArguments.getStructuredS3Path(), sourceName, tableName);
        String curatedPath = tablePath(jobArguments.getCuratedS3Path(), sourceName, tableName);
        Dataset<Row> structured = s3DataProvider.getBatchSourceData(sparkSession, structuredPath);
        Dataset<Row> curated = s3DataProvider.getBatchSourceData(sparkSession, curatedPath);


        logger.info("Reading Nomis count");
        long nomisCount = nomisDataProvider.getTableCount(oracleFullTableName);
        logger.info("Reading Structured count");
        long structuredCount = structured.count();
        logger.info("Reading Curated count");
        long curatedCount = curated.count();

        return new CurrentStateCountTableResult(nomisCount, structuredCount, curatedCount);
    }


    public void reconcileDataOrThrow(SparkSession sparkSession) throws DataReconciliationFailureException {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(jobArguments.getConfigKey());
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        CurrentStateCountResults results = new CurrentStateCountResults();
        allSourceReferences.forEach(sourceReference -> {
            results.put(sourceReference, currentStateCounts(sparkSession, sourceReference));
        });
        logger.info(results.summary());
        if (!results.countsMatch()) {
            throw new DataReconciliationFailureException(results);
        }
    }
}

