package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.HiveSchemaServiceException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;
import static uk.gov.justice.digital.common.CommonDataFields.withCheckpointField;
import static uk.gov.justice.digital.common.RegexPatterns.TABLE_NAME_PATTERN;
import static uk.gov.justice.digital.common.RegexPatterns.tableNameRegex;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

@Singleton
public class HiveTableService {

    private static final Logger logger = LoggerFactory.getLogger(HiveTableService.class);

    private final JobArguments jobArguments;
    private final SourceReferenceService sourceReferenceService;
    private final DataStorageService storageService;
    private final GlueClient glueClient;

    @Inject
    public HiveTableService(
            JobArguments jobArguments,
            SourceReferenceService sourceReferenceService,
            DataStorageService storageService,
            GlueClient glueClient
    ) {
        this.jobArguments = jobArguments;
        this.sourceReferenceService = sourceReferenceService;
        this.storageService = storageService;
        this.glueClient = glueClient;
    }

    public Set<ImmutablePair<String, String>> replaceTables(ImmutableSet<ImmutablePair<String, String>> tables) {

        Set<ImmutablePair<String, String>> failedTables = new HashSet<>();
        List<SourceReference> sourceReferences = getSourceReferences(tables);

        for (SourceReference sourceReference : sourceReferences) {
            String sourceName = sourceReference.getSource();
            String tableName = sourceReference.getTable();

            String hiveTableName = sourceName + "_" + tableName;

            logger.info("Processing {}", hiveTableName);
            try {
                validateTableName(hiveTableName);
                StructType schema = sourceReference.getSchema();
                SourceReference.PrimaryKey primaryKey = sourceReference.getPrimaryKey();
                SourceReference.SensitiveColumns sensitiveColumns = sourceReference.getSensitiveColumns();

                String rawArchivePath = createValidatedPath(jobArguments.getRawArchiveS3Path(), sourceName, tableName);
                StructType rawSchema = withCheckpointField(withMetadataFields(schema));
                replaceParquetInputTables(jobArguments.getRawArchiveDatabase(), hiveTableName, rawArchivePath, rawSchema, primaryKey, sensitiveColumns);

                String structuredPath = createValidatedPath(jobArguments.getStructuredS3Path(), sourceName, tableName);
                replaceSymlinkInputTables(jobArguments.getStructuredDatabase(), hiveTableName, structuredPath, schema, primaryKey);

                String curatedPath = createValidatedPath(jobArguments.getCuratedS3Path(), sourceName, tableName);
                replaceSymlinkInputTables(jobArguments.getCuratedDatabase(), hiveTableName, curatedPath, schema, primaryKey);

                replaceSymlinkInputTables(jobArguments.getPrisonsDatabase(), hiveTableName, curatedPath, schema, primaryKey);
            } catch (Exception e) {
                logger.error("Failed to replace Hive table {}", hiveTableName, e);
                failedTables.add(ImmutablePair.of(sourceName, tableName));
            }

        }

        return failedTables;
    }

    public Set<ImmutablePair<String, String>> switchPrisonsTableDataSource(SparkSession spark, ImmutableSet<ImmutablePair<String, String>> tables) {

        Set<ImmutablePair<String, String>> failedTables = new HashSet<>();
        List<SourceReference> sourceReferences = getSourceReferences(tables);

        for (SourceReference sourceReference : sourceReferences) {
            String sourceName = sourceReference.getSource();
            String tableName = sourceReference.getTable();

            String hiveTableName = sourceName + "_" + tableName;
            String targetS3Path = jobArguments.getPrisonsDataSwitchTargetS3Path();

            logger.info("Processing {}", hiveTableName);
            try {
                validateTableName(hiveTableName);
                StructType schema = sourceReference.getSchema();
                SourceReference.PrimaryKey primaryKey = sourceReference.getPrimaryKey();

                String dataPath = createValidatedPath(targetS3Path, sourceName, tableName);

                replaceSymlinkInputTables(jobArguments.getPrisonsDatabase(), hiveTableName, dataPath, schema, primaryKey);
                storageService.updateDeltaManifestForTable(spark, dataPath);
            } catch (Exception e) {
                logger.error("Failed to point Hive table {} to {}", hiveTableName, targetS3Path, e);
                failedTables.add(ImmutablePair.of(sourceName, tableName));
            }
        }

        return failedTables;
    }

    @NotNull
    private List<SourceReference> getSourceReferences(ImmutableSet<ImmutablePair<String, String>> tables) {
        logger.info("Retrieving all schemas in registry");
        List<SourceReference> sourceReferences = sourceReferenceService.getAllSourceReferences(tables);

        if (sourceReferences.isEmpty()) {
            throw new HiveSchemaServiceException("No schemas retrieved from registry");
        }
        return sourceReferences;
    }

    private void replaceParquetInputTables(
            String databaseName,
            String tableName,
            String dataPath,
            StructType schema,
            SourceReference.PrimaryKey primaryKey,
            SourceReference.SensitiveColumns sensitiveColumns
    ) {
        glueClient.deleteTable(databaseName, tableName);
        glueClient.createParquetTable(databaseName, tableName, dataPath, schema, primaryKey, sensitiveColumns);
    }

    private void replaceSymlinkInputTables(String databaseName, String tableName, String curatedPath, StructType schema, SourceReference.PrimaryKey primaryKey) {
        glueClient.deleteTable(databaseName, tableName);
        glueClient.createTableWithSymlink(databaseName, tableName, curatedPath, schema, primaryKey);
    }

    private void validateTableName(String tableName) throws HiveSchemaServiceException {
        if (!tableNameRegex.matcher(tableName).matches()) {
            String errorMessage = String.format("Table name %s is not supported. Use %s", tableName, TABLE_NAME_PATTERN);
            throw new HiveSchemaServiceException(errorMessage);
        }
    }

}
