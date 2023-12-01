package uk.gov.justice.digital.service;

import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.glue.GlueHiveTableClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.HiveSchemaServiceException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

@Singleton
public class HiveSchemaService {

    private static final Logger logger = LoggerFactory.getLogger(HiveSchemaService.class);

    private static final String TABLE_NAME_PATTERN = "^[a-z_0-9]*$";
    private static final Pattern tableNameRegex = Pattern.compile(TABLE_NAME_PATTERN);

    private final JobArguments jobArguments;
    private final SourceReferenceService sourceReferenceService;
    private final GlueHiveTableClient glueHiveTableClient;

    @Inject
    public HiveSchemaService(
            JobArguments jobArguments,
            SourceReferenceService sourceReferenceService,
            GlueHiveTableClient glueHiveTableClient
    ) {
        this.jobArguments = jobArguments;
        this.sourceReferenceService = sourceReferenceService;
        this.glueHiveTableClient = glueHiveTableClient;
    }

    public Set<String> replaceTables(Set<String> schemaGroup) {

        Set<String> failedTables = new HashSet<>();

        logger.info("Retrieving all schemas in registry");
        List<SourceReference> sourceReferences = sourceReferenceService.getAllSourceReferences(schemaGroup);

        if (sourceReferences.isEmpty()) {
            throw new HiveSchemaServiceException("No schemas retrieved from registry");
        }

        for (SourceReference sourceReference : sourceReferences) {
            String sourceName = sourceReference.getSource();
            String tableName = sourceReference.getTable();

            String hiveTableName = sourceName + "_" + tableName;

            logger.info("Processing {}", hiveTableName);
            try {
                validateTableName(hiveTableName);
                StructType schema = sourceReference.getSchema();

                String rawArchivePath = createValidatedPath(jobArguments.getRawArchiveS3Path(), sourceName, tableName);
                StructType rawSchema = withMetadataFields(schema);
                replaceParquetInputTables(jobArguments.getRawArchiveDatabase(), hiveTableName, rawArchivePath, rawSchema);

                String structuredPath = createValidatedPath(jobArguments.getStructuredS3Path(), sourceName, tableName);
                replaceSymlinkInputTables(jobArguments.getStructuredDatabase(), hiveTableName, structuredPath, schema);

                String curatedPath = createValidatedPath(jobArguments.getCuratedS3Path(), sourceName, tableName);
                replaceSymlinkInputTables(jobArguments.getCuratedDatabase(), hiveTableName, curatedPath, schema);

                replaceSymlinkInputTables(jobArguments.getPrisonsDatabase(), hiveTableName, curatedPath, schema);
            } catch (Exception e) {
                logger.error("Failed to replace Hive table {}", hiveTableName);
                failedTables.add(hiveTableName);
            }

        }

        return failedTables;
    }

    private void replaceParquetInputTables(String databaseName, String tableName, String dataPath, StructType schema) {
        glueHiveTableClient.deleteTable(databaseName, tableName);
        glueHiveTableClient.createParquetTable(databaseName, tableName, dataPath, schema);
    }

    private void replaceSymlinkInputTables(String databaseName, String tableName, String curatedPath, StructType schema) {
        glueHiveTableClient.deleteTable(databaseName, tableName);
        glueHiveTableClient.createTableWithSymlink(databaseName, tableName, curatedPath, schema);
    }

    private void validateTableName(String tableName) throws HiveSchemaServiceException {
        if (!tableNameRegex.matcher(tableName).matches()) {
            String errorMessage = String.format("Table name %s is not supported. Use %s", tableName, TABLE_NAME_PATTERN);
            throw new HiveSchemaServiceException(errorMessage);
        }
    }

}