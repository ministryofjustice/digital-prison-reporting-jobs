package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.AWSGlueException;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.BatchStopJobRunRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.GetJobRunsRequest;
import com.amazonaws.services.glue.model.GetJobRunRequest;
import com.amazonaws.services.glue.model.JobRun;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.GlueClientException;

import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
public class GlueClient {

    private static final Logger logger = LoggerFactory.getLogger(GlueClient.class);

    public static final String MAPRED_PARQUET_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
    public static final String SYMLINK_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat";

    private final AWSGlue awsGlue;

    @Inject
    public GlueClient(GlueClientProvider glueClientProvider) {
        this.awsGlue = glueClientProvider.getClient();
    }

    public void createParquetTable(
            String database,
            String table,
            String dataPath,
            StructType schema,
            SourceReference.PrimaryKey primaryKey,
            SourceReference.SensitiveColumns sensitiveColumns
        ) throws AWSGlueException {

        val storageDescriptor = createStorageDescriptor(dataPath, MAPRED_PARQUET_INPUT_FORMAT, schema, primaryKey);

        String sensitiveColumnsFormatted = sensitiveColumns
                .getSensitiveColumnNames()
                .stream()
                .map(col -> "'" + col.toLowerCase() + "'")
                .collect(Collectors.joining(",", "[", "]"));

        String extractionKeyFormatted = String.join(",", Stream.of(TIMESTAMP, CHECKPOINT_COL).map(String::toLowerCase)
                .collect(Collectors.toCollection(HashSet::new)));

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

    public void createTableWithSymlink(String database, String table, String dataPath, StructType schema, SourceReference.PrimaryKey primaryKey) throws AWSGlueException {
        String location = dataPath + "/_symlink_format_manifest";
        val storageDescriptor = createStorageDescriptor(location, SYMLINK_INPUT_FORMAT, schema, primaryKey);
        val params = Collections.singletonMap("classification", "parquet");
        createTable(database, table, storageDescriptor, params);
    }

    public void deleteTable(String database, String table) throws AWSGlueException {
        DeleteTableRequest deleteTableRequest = new DeleteTableRequest()
                .withDatabaseName(database)
                .withName(table);

        try {
            logger.info("Deleting table {}.{}", database, table);
            awsGlue.deleteTable(deleteTableRequest);
            logger.info("Successfully deleted table {}.{}", database, table);
        } catch (EntityNotFoundException e) {
            logger.info("Did not delete non-existent table {}.{}", database, table);
        }
    }

    public void stopJob(String jobName, int waitIntervalSeconds, int maxAttempts) {
        BatchStopJobRunRequest batchStopJobRunRequest = new BatchStopJobRunRequest().withJobName(jobName);
        Optional<String> optionalRunningJobId = getRunningJobId(jobName);

        optionalRunningJobId.ifPresent(
                runId -> {
                    batchStopJobRunRequest.withJobRunIds(runId);
                    logger.info("Stopping job {} with runId {}", jobName, runId);
                    awsGlue.batchStopJobRun(batchStopJobRunRequest);

                    try {
                        ensureState(jobName, runId, "STOPPED", waitIntervalSeconds, maxAttempts);
                    } catch (InterruptedException e) {
                        logger.error("Error while ensuring job {} has stopped", jobName, e);
                        Thread.currentThread().interrupt();
                    }
                }
        );
    }

    private void createTable(String database, String table, StorageDescriptor storageDescriptor, Map<String, String> params) {
        CreateTableRequest createTableRequest = getCreateTableRequest(database, table, storageDescriptor, params);

        logger.info("Creating table {}.{}", database, table);
        awsGlue.createTable(createTableRequest);
        logger.info("Successfully created table {}.{}", database, table);
    }

    private StorageDescriptor createStorageDescriptor(String location, String inputFormat, StructType schema, SourceReference.PrimaryKey primaryKey) {
        return new StorageDescriptor()
                .withColumns(getColumnsAndModifyTypes(schema, primaryKey))
                .withLocation(location)
                .withInputFormat(inputFormat)
                .withOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .withSerdeInfo(
                        new SerDeInfo()
                                .withSerializationLibrary("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
                                .withParameters(Collections.singletonMap("serialization.format", ","))
                )
                .withCompressed(false)
                .withNumberOfBuckets(0)
                .withStoredAsSubDirectories(false);
    }

    private CreateTableRequest getCreateTableRequest(String database, String table, StorageDescriptor storageDescriptor, Map<String, String> params) {
        return new CreateTableRequest()
                .withDatabaseName(database)
                .withTableInput(new TableInput()
                        .withName(table)
                        .withTableType("EXTERNAL_TABLE")
                        .withParameters(params)
                        .withStorageDescriptor(storageDescriptor)
                );
    }

    private List<Column> getColumnsAndModifyTypes(StructType schema, SourceReference.PrimaryKey primaryKey) {
        Collection<String> keyColumnNames = primaryKey.getKeyColumnNames();
        val columns = new ArrayList<Column>();
        for (StructField field : schema.fields()) {
            val column = new Column().withName(field.name()).withType(field.dataType().typeName());
            // Null type not supported in AWS Glue Catalog.
            // Numerical type mappings should be explicit and not automatically selected.
            switch (column.getType()) {
                case "long": column.setType("bigint");
                    break;
                case "short": column.setType("smallint");
                    break;
                case "integer": column.setType("int");
                    break;
                case "byte": column.setType("tinyint");
                    break;
                default:
                    break;
            }

            if (keyColumnNames.contains(column.getName())) column.setComment("primary_key");
            columns.add(column);
        }
        return columns;
    }

    @NotNull
    private Optional<String> getRunningJobId(String jobName) {
        logger.info("Retrieving the Id of the running instance of job {}", jobName);
        GetJobRunsRequest getJobRunsRequest = new GetJobRunsRequest().withJobName(jobName).withMaxResults(200);
        List<JobRun> jobRuns = awsGlue.getJobRuns(getJobRunsRequest).getJobRuns();
        return jobRuns.stream()
                .filter(jobRun -> jobRun.getJobRunState().equalsIgnoreCase("RUNNING"))
                .map(JobRun::getId)
                .findFirst();
    }

    private void ensureState(String jobName, String runId, String state, int waitIntervalSeconds, int maxAttempts) throws InterruptedException {
        GetJobRunRequest getJobRunRequest;
        String jobRunState;

        for (int attempts = 0; attempts < maxAttempts; attempts++) {
            logger.info("Ensuring job {} with runId {} is in {} state. Attempt {}", jobName, runId, state, attempts);
            getJobRunRequest = new GetJobRunRequest().withJobName(jobName).withRunId(runId);
            jobRunState = awsGlue.getJobRun(getJobRunRequest).getJobRun().getJobRunState();
            TimeUnit.SECONDS.sleep(waitIntervalSeconds);

            if (jobRunState.equalsIgnoreCase(state)) return;
        }

        String errorMessage = String.format("Exhausted attempts waiting for job %s with runId %s to be %s", jobName, runId, state);
        throw new GlueClientException(errorMessage);
    }
}
