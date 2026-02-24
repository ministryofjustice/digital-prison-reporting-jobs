package uk.gov.justice.digital.client.glue;

import software.amazon.awssdk.services.glue.model.Connection;
import software.amazon.awssdk.services.glue.model.GetConnectionRequest;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.BatchStopJobRunRequest;
import software.amazon.awssdk.services.glue.model.JobRunState;
import software.amazon.awssdk.services.glue.model.StartTriggerRequest;
import software.amazon.awssdk.services.glue.model.StopTriggerRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.GetJobRunsRequest;
import software.amazon.awssdk.services.glue.model.JobRun;
import software.amazon.awssdk.services.glue.model.GetJobRunRequest;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.GlueClientException;

import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.CHECKPOINT_COL;

@Singleton
public class DefaultGlueClient {

    private static final Logger logger = LoggerFactory.getLogger(DefaultGlueClient.class);

    public static final String MAPRED_PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String SYMLINK_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat";

    private final GlueClient awsGlue;

    @Inject
    public DefaultGlueClient(GlueClientProvider glueClientProvider) {
        this.awsGlue = glueClientProvider.getClient();
    }

    /**
     * Retrieves a Glue Data Connection by name.
     */
    public Connection getConnection(String connectionName) {
        GetConnectionRequest request = GetConnectionRequest.builder().name(connectionName).build();
        return awsGlue.getConnection(request).connection();
    }

    public void createParquetTable(
            String database,
            String table,
            String dataPath,
            StructType schema,
            SourceReference.PrimaryKey primaryKey,
            SourceReference.SensitiveColumns sensitiveColumns
    ) throws GlueException {

        val storageDescriptor = createStorageDescriptor(dataPath, MAPRED_PARQUET_INPUT_FORMAT, schema, primaryKey);

        String sensitiveColumnsFormatted = sensitiveColumns
                .getSensitiveColumnNames()
                .stream()
                .map(col -> "'" + col.toLowerCase() + "'")
                .collect(Collectors.joining(",", "[", "]"));

        String extractionKeyFormatted = Stream.of(TIMESTAMP, CHECKPOINT_COL).map(String::toLowerCase)
                .collect(Collectors.joining(","));

        String primaryKeysFormatted = primaryKey.getKeyColumnNames()
                .stream()
                .map(String::toLowerCase)
                .collect(Collectors.joining(","));

        val params = new HashMap<String, String>();

        params.put("classification", "parquet");
        params.put("extraction_timestamp_column_name", TIMESTAMP.toLowerCase());
        params.put("extraction_operation_column_name", OPERATION.toLowerCase());
        params.put("sensitive_columns", sensitiveColumnsFormatted);
        params.put("extraction_key", extractionKeyFormatted);
        params.put("source_primary_key", primaryKeysFormatted);
        createTable(database, table, storageDescriptor, params);
    }

    public void createTableWithSymlink(String database, String table, String dataPath, StructType schema, SourceReference.PrimaryKey primaryKey) throws GlueException {
        String location = dataPath + "/_symlink_format_manifest";
        val storageDescriptor = createStorageDescriptor(location, SYMLINK_INPUT_FORMAT, schema, primaryKey);
        val params = Collections.singletonMap("classification", "parquet");
        createTable(database, table, storageDescriptor, params);
    }

    public void deleteTable(String database, String table) throws GlueException {
        DeleteTableRequest deleteTableRequest = DeleteTableRequest
                .builder()
                .databaseName(database)
                .name(table)
                .build();

        try {
            logger.info("Deleting table {}.{}", database, table);
            awsGlue.deleteTable(deleteTableRequest);
            logger.info("Successfully deleted table {}.{}", database, table);
        } catch (EntityNotFoundException e) {
            logger.info("Did not delete non-existent table {}.{}", database, table);
        }
    }

    public void stopJob(String jobName, int waitIntervalSeconds, int maxAttempts) {
        val batchStopJobRunRequest = BatchStopJobRunRequest.builder().jobName(jobName);
        Optional<String> optionalRunningJobId = getRunningJobId(jobName);

        optionalRunningJobId.ifPresent(
                runId -> {
                    batchStopJobRunRequest.jobRunIds(runId);
                    logger.info("Stopping job {} with runId {}", jobName, runId);
                    awsGlue.batchStopJobRun(batchStopJobRunRequest.build());

                    try {
                        ensureState(jobName, runId, JobRunState.STOPPED, waitIntervalSeconds, maxAttempts);
                    } catch (InterruptedException e) {
                        logger.error("Error while ensuring job {} has stopped", jobName, e);
                        Thread.currentThread().interrupt();
                    }
                }
        );
    }

    public void activateTrigger(String triggerName) {
        awsGlue.startTrigger(StartTriggerRequest.builder().name(triggerName).build());
    }

    public void deactivateTrigger(String triggerName) {
        awsGlue.stopTrigger(StopTriggerRequest.builder().name(triggerName).build());
    }

    private void createTable(String database, String table, StorageDescriptor storageDescriptor, Map<String, String> params) {
        CreateTableRequest createTableRequest = getCreateTableRequest(database, table, storageDescriptor, params);

        logger.info("Creating table {}.{}", database, table);
        awsGlue.createTable(createTableRequest);
        logger.info("Successfully created table {}.{}", database, table);
    }

    private StorageDescriptor createStorageDescriptor(String location, String inputFormat, StructType schema, SourceReference.PrimaryKey primaryKey) {
        return StorageDescriptor
                .builder()
                .columns(getColumnsAndModifyTypes(schema, primaryKey))
                .location(location)
                .inputFormat(inputFormat)
                .outputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .serdeInfo(
                        SerDeInfo
                                .builder()
                                .serializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                                .parameters(Collections.singletonMap("serialization.format", ","))
                                .build()
                )
                .compressed(false)
                .numberOfBuckets(0)
                .storedAsSubDirectories(false)
                .build();
    }

    private CreateTableRequest getCreateTableRequest(String database, String table, StorageDescriptor storageDescriptor, Map<String, String> params) {
        return CreateTableRequest.builder()
                .databaseName(database)
                .tableInput(builder -> builder.name(table).tableType("EXTERNAL_TABLE").parameters(params).storageDescriptor(storageDescriptor))
                .build();
    }

    private List<Column> getColumnsAndModifyTypes(StructType schema, SourceReference.PrimaryKey primaryKey) {
        Collection<String> keyColumnNames = primaryKey.getKeyColumnNames();
        val columns = new ArrayList<Column>();
        for (StructField field : schema.fields()) {
            val column = Column.builder().name(field.name()).type(field.dataType().typeName()).build();
            // Null type not supported in AWS Glue Catalog.
            // Numerical type mappings should be explicit and not automatically selected.
            val updatedColumn = switch (column.type()) {
                case "long" -> Column.builder().name(field.name()).type("bigint");
                case "short" -> Column.builder().name(field.name()).type("smallint");
                case "integer" -> Column.builder().name(field.name()).type("int");
                case "byte" -> Column.builder().name(field.name()).type("tinyint");
                default -> Column.builder().name(field.name()).type(column.type());
            };

            if (keyColumnNames.contains(column.name())) updatedColumn.comment("primary_key");
            columns.add(updatedColumn.build());
        }
        return columns;
    }

    @NotNull
    private Optional<String> getRunningJobId(String jobName) {
        logger.info("Retrieving the Id of the running instance of job {}", jobName);
        GetJobRunsRequest getJobRunsRequest = GetJobRunsRequest.builder().jobName(jobName).maxResults(200).build();
        List<JobRun> jobRuns = awsGlue.getJobRuns(getJobRunsRequest).jobRuns();
        return jobRuns.stream()
                .filter(jobRun -> jobRun.jobRunState().equals(JobRunState.RUNNING))
                .map(JobRun::id)
                .findFirst();
    }

    private void ensureState(String jobName, String runId, JobRunState state, int waitIntervalSeconds, int maxAttempts) throws InterruptedException {
        GetJobRunRequest getJobRunRequest;
        JobRunState jobRunState;

        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            logger.info("Ensuring job {} with runId {} is in {} state. Attempt {}", jobName, runId, state, attempts);
            getJobRunRequest = GetJobRunRequest.builder().jobName(jobName).runId(runId).build();
            jobRunState = awsGlue.getJobRun(getJobRunRequest).jobRun().jobRunState();
            TimeUnit.SECONDS.sleep(waitIntervalSeconds);

            if (jobRunState.equals(state)) return;
        }

        String errorMessage = String.format("Exhausted attempts waiting for job %s with runId %s to be %s", jobName, runId, state);
        throw new GlueClientException(errorMessage);
    }
}
