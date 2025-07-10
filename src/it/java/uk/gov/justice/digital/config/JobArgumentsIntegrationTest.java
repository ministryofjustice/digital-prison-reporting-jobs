package uk.gov.justice.digital.config;

import com.google.common.collect.ImmutableSet;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.CommandLinePropertySource;
import io.micronaut.context.env.Environment;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.RegexPatterns.jsonOrParquetFileRegex;
import static uk.gov.justice.digital.common.RegexPatterns.matchAllFiles;
import static uk.gov.justice.digital.common.RegexPatterns.parquetFileRegex;
import static uk.gov.justice.digital.config.JobArguments.DEFAULT_CDC_TRIGGER_INTERVAL_SECONDS;
import static uk.gov.justice.digital.config.JobArguments.DEFAULT_RAW_FILE_RETENTION_PERIOD_AMOUNT;
import static uk.gov.justice.digital.config.JobArguments.DEFAULT_SPARK_BROADCAST_TIMEOUT_SECONDS;
import static uk.gov.justice.digital.config.JobArguments.DEFAULT_SPARK_SQL_MAX_RECORDS_PER_FILE;
import static uk.gov.justice.digital.config.JobArguments.RECONCILIATION_CHECKS_TO_RUN_DEFAULT;
import static uk.gov.justice.digital.config.JobArguments.STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CHANGE_DATA_COUNTS;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CURRENT_STATE_COUNTS;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.PRIMARY_KEY_RECONCILIATION;

class JobArgumentsIntegrationTest {

    private static final Map<String, String> testArguments = Stream.of(new String[][] {
            { JobArguments.CONFIG_S3_BUCKET, "test-config-bucket" },
            { JobArguments.CONFIG_KEY, "test-config" },
            { JobArguments.AWS_REGION, "test-region" },
            { JobArguments.CURATED_S3_PATH, "s3://somepath/curated" },
            { JobArguments.PRISONS_DATA_SWITCH_TARGET_S3_PATH, "s3://somepath/target" },
            { JobArguments.ENABLE_STREAMING_SOURCE_ARCHIVING, "true" },
            { JobArguments.PROCESSED_RAW_FILES_PATH, "processed/raw/files" },
            { JobArguments.DOMAIN_CATALOG_DATABASE_NAME, "SomeDomainCatalogName" },
            { JobArguments.DOMAIN_NAME, "test_domain_name" },
            { JobArguments.DOMAIN_OPERATION, "insert" },
            { JobArguments.DOMAIN_REGISTRY, "test_registry" },
            { JobArguments.DOMAIN_TARGET_PATH, "s3://somepath/domain/target" },
            { JobArguments.DOMAIN_TABLE_NAME, "test_table" },
            { JobArguments.RAW_S3_PATH, "s3://somepath/raw" },
            { JobArguments.RAW_ARCHIVE_S3_PATH, "s3://somepath/raw-archive" },
            { JobArguments.STRUCTURED_S3_PATH, "s3://somepath/structured" },
            { JobArguments.VIOLATIONS_S3_PATH, "s3://somepath/violations" },
            { JobArguments.TEMP_RELOAD_S3_PATH, "s3://somepath/temp-reload" },
            { JobArguments.TEMP_RELOAD_OUTPUT_FOLDER, "temp-reload-output-folder" },
            { JobArguments.RAW_ARCHIVE_DATABASE, "raw_archive" },
            { JobArguments.STRUCTURED_DATABASE, "structured" },
            { JobArguments.CURATED_DATABASE, "curated" },
            { JobArguments.PRISONS_DATABASE, "prisons" },
            { JobArguments.AWS_DYNAMODB_ENDPOINT_URL, "https://dynamodb.example.com" },
            { JobArguments.CONTRACT_REGISTRY_NAME, "SomeContractRegistryName" },
            { JobArguments.SCHEMA_CACHE_MAX_SIZE, "0" },
            { JobArguments.SCHEMA_CACHE_EXPIRY_IN_MINUTES, "0" },
            { JobArguments.CHECKPOINT_LOCATION, "s3://somepath/checkpoint/app-name" },
            { JobArguments.KINESIS_STREAM_ARN, "arn:aws:kinesis:eu-west-2:123456:stream/dpr-kinesis-ingestor-env" },
            { JobArguments.KINESIS_STARTING_POSITION, "trim_horizon" },
            { JobArguments.ADD_IDLE_TIME_BETWEEN_READS, "true" },
            { JobArguments.BATCH_MAX_RETRIES, "5" },
            { JobArguments.LOG_LEVEL, "debug" },
            { JobArguments.MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH, "1" },
            { JobArguments.FILE_TRANSFER_SOURCE_BUCKET_NAME, "dpr-source-bucket" },
            { JobArguments.FILE_TRANSFER_DESTINATION_BUCKET_NAME, "dpr-destination-bucket" },
            { JobArguments.FILE_SOURCE_PREFIX, "dpr-source-prefix" },
            { JobArguments.STOP_GLUE_INSTANCE_JOB_NAME, "dpr-glue-job-name" },
            { JobArguments.DMS_REPLICATION_TASK_ID, "dpr-dms-task-id" },
            { JobArguments.CDC_DMS_REPLICATION_TASK_ID, "cdc-dpr-dms-task-id" },
            { JobArguments.RELOAD_JOB_USE_NOW_AS_CHECKPOINT, "true" },
            { JobArguments.ORCHESTRATION_WAIT_INTERVAL_SECONDS, "5" },
            { JobArguments.ORCHESTRATION_MAX_ATTEMPTS, "10" },
            { JobArguments.MAX_S3_PAGE_SIZE, "100" },
            { JobArguments.CLEAN_CDC_CHECKPOINT, "false" },
            { JobArguments.CDC_TRIGGER_INTERVAL_SECONDS, "30" },
            { JobArguments.GLUE_TRIGGER_NAME, "dpr-glue-trigger-name" },
            { JobArguments.SPARK_BROADCAST_TIMEOUT_SECONDS, "60" },
            { JobArguments.DISABLE_AUTO_BROADCAST_JOIN_THRESHOLD, "false" },
            { JobArguments.OPERATIONAL_DATA_STORE_GLUE_CONNECTION_NAME, "some-connection-name" },
            { JobArguments.OPERATIONAL_DATA_STORE_WRITE_ENABLED, "true" },
            { JobArguments.OPERATIONAL_DATA_STORE_LOADING_SCHEMA_NAME, "some_schema" },
            { JobArguments.OPERATIONAL_DATA_STORE_TABLES_TO_WRITE_TABLE_NAME, "configuration.datahub_managed_tables" },
            { JobArguments.OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE, "10000" },
            { JobArguments.RECONCILIATION_DATASOURCE_GLUE_CONNECTION_NAME, "my-connection" },
            { JobArguments.RECONCILIATION_DATASOURCE_SOURCE_SCHEMA_NAME, "OMS_OWNER" },
            { JobArguments.RECONCILIATION_CLOUDWATCH_METRICS_NAMESPACE, "SomeNamespace" },
            { JobArguments.RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE, "0.02" },
            { JobArguments.RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_ABSOLUTE, "12" },
            { JobArguments.RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE, "0.01" },
            { JobArguments.RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_ABSOLUTE, "5" },
            { JobArguments.SECRET_ID, "test_secret_id" },
            { JobArguments.ADJUST_SPARK_MEMORY, "true" },
            { JobArguments.SPARK_SQL_MAX_RECORDS_PER_FILE, "50000" },
    }).collect(Collectors.toMap(e -> e[0], e -> e[1]));

    private static final JobArguments validArguments = new JobArguments(givenAContextWithArguments(testArguments));
    private static final JobArguments emptyArguments = new JobArguments(givenAContextWithNoArguments());

    @Test
    public void shouldReturnCorrectValueForEachSupportedArgument() {
        Map<String, String> actualArguments = Stream.of(new Object[][] {
                { JobArguments.CONFIG_S3_BUCKET, validArguments.getConfigS3Bucket() },
                { JobArguments.CONFIG_KEY, validArguments.getConfigKey() },
                { JobArguments.AWS_DYNAMODB_ENDPOINT_URL, validArguments.getAwsDynamoDBEndpointUrl() },
                { JobArguments.AWS_REGION, validArguments.getAwsRegion() },
                { JobArguments.CURATED_S3_PATH, validArguments.getCuratedS3Path() },
                { JobArguments.PRISONS_DATA_SWITCH_TARGET_S3_PATH, validArguments.getPrisonsDataSwitchTargetS3Path() },
                { JobArguments.ENABLE_STREAMING_SOURCE_ARCHIVING, validArguments.enableStreamingSourceArchiving() },
                { JobArguments.PROCESSED_RAW_FILES_PATH, validArguments.getProcessedRawFilesPath() },
                { JobArguments.DOMAIN_CATALOG_DATABASE_NAME, validArguments.getDomainCatalogDatabaseName() },
                { JobArguments.DOMAIN_NAME, validArguments.getDomainName() },
                { JobArguments.DOMAIN_OPERATION, validArguments.getDomainOperation() },
                { JobArguments.DOMAIN_REGISTRY, validArguments.getDomainRegistry() },
                { JobArguments.DOMAIN_TARGET_PATH, validArguments.getDomainTargetPath() },
                { JobArguments.DOMAIN_TABLE_NAME, validArguments.getDomainTableName() },
                { JobArguments.RAW_S3_PATH, validArguments.getRawS3Path() },
                { JobArguments.RAW_ARCHIVE_S3_PATH, validArguments.getRawArchiveS3Path() },
                { JobArguments.STRUCTURED_S3_PATH, validArguments.getStructuredS3Path() },
                { JobArguments.VIOLATIONS_S3_PATH, validArguments.getViolationsS3Path() },
                { JobArguments.TEMP_RELOAD_S3_PATH, validArguments.getTempReloadS3Path() },
                { JobArguments.TEMP_RELOAD_OUTPUT_FOLDER, validArguments.getTempReloadOutputFolder() },
                { JobArguments.RAW_ARCHIVE_DATABASE, validArguments.getRawArchiveDatabase() },
                { JobArguments.STRUCTURED_DATABASE, validArguments.getStructuredDatabase() },
                { JobArguments.CURATED_DATABASE, validArguments.getCuratedDatabase() },
                { JobArguments.PRISONS_DATABASE, validArguments.getPrisonsDatabase() },
                { JobArguments.CONTRACT_REGISTRY_NAME, validArguments.getContractRegistryName() },
                { JobArguments.SCHEMA_CACHE_MAX_SIZE, validArguments.getSchemaCacheMaxSize() },
                { JobArguments.SCHEMA_CACHE_EXPIRY_IN_MINUTES, validArguments.getSchemaCacheExpiryInMinutes() },
                { JobArguments.CHECKPOINT_LOCATION, validArguments.getCheckpointLocation() },
                { JobArguments.KINESIS_STREAM_ARN, validArguments.getKinesisStreamArn() },
                { JobArguments.KINESIS_STARTING_POSITION, validArguments.getKinesisStartingPosition() },
                { JobArguments.ADD_IDLE_TIME_BETWEEN_READS, validArguments.addIdleTimeBetweenReads() },
                { JobArguments.BATCH_MAX_RETRIES, Integer.toString(validArguments.getBatchMaxRetries()) },
                { JobArguments.LOG_LEVEL, validArguments.getLogLevel().toString().toLowerCase() },
                { JobArguments.MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH, Integer.toString(validArguments.getMaintenanceListTableRecurseMaxDepth()) },
                { JobArguments.FILE_TRANSFER_SOURCE_BUCKET_NAME, validArguments.getTransferSourceBucket() },
                { JobArguments.FILE_TRANSFER_DESTINATION_BUCKET_NAME, validArguments.getTransferDestinationBucket() },
                { JobArguments.FILE_SOURCE_PREFIX, validArguments.getSourcePrefix() },
                { JobArguments.STOP_GLUE_INSTANCE_JOB_NAME, validArguments.getStopGlueInstanceJobName() },
                { JobArguments.DMS_REPLICATION_TASK_ID, validArguments.getDmsTaskId() },
                { JobArguments.CDC_DMS_REPLICATION_TASK_ID, validArguments.getCdcDmsTaskId() },
                { JobArguments.RELOAD_JOB_USE_NOW_AS_CHECKPOINT, validArguments.shouldUseNowAsCheckpointForReloadJob() },
                { JobArguments.ORCHESTRATION_WAIT_INTERVAL_SECONDS, validArguments.orchestrationWaitIntervalSeconds() },
                { JobArguments.ORCHESTRATION_MAX_ATTEMPTS, validArguments.orchestrationMaxAttempts() },
                { JobArguments.MAX_S3_PAGE_SIZE, validArguments.getMaxObjectsPerPage() },
                { JobArguments.CLEAN_CDC_CHECKPOINT, validArguments.cleanCdcCheckpoint() },
                { JobArguments.CDC_TRIGGER_INTERVAL_SECONDS, validArguments.getCdcTriggerIntervalSeconds() },
                { JobArguments.GLUE_TRIGGER_NAME, validArguments.getGlueTriggerName() },
                { JobArguments.SPARK_BROADCAST_TIMEOUT_SECONDS, validArguments.getBroadcastTimeoutSeconds() },
                { JobArguments.DISABLE_AUTO_BROADCAST_JOIN_THRESHOLD, validArguments.disableAutoBroadcastJoinThreshold() },
                { JobArguments.OPERATIONAL_DATA_STORE_GLUE_CONNECTION_NAME, validArguments.getOperationalDataStoreGlueConnectionName() },
                { JobArguments.OPERATIONAL_DATA_STORE_WRITE_ENABLED, validArguments.isOperationalDataStoreWriteEnabled() },
                { JobArguments.OPERATIONAL_DATA_STORE_LOADING_SCHEMA_NAME, validArguments.getOperationalDataStoreLoadingSchemaName() },
                { JobArguments.OPERATIONAL_DATA_STORE_TABLES_TO_WRITE_TABLE_NAME, validArguments.getOperationalDataStoreTablesToWriteTableName() },
                { JobArguments.OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE, Long.toString(validArguments.getOperationalDataStoreJdbcBatchSize()) },
                { JobArguments.RECONCILIATION_DATASOURCE_GLUE_CONNECTION_NAME, validArguments.getReconciliationDataSourceGlueConnectionName() },
                { JobArguments.RECONCILIATION_DATASOURCE_SOURCE_SCHEMA_NAME, validArguments.getReconciliationDataSourceSourceSchemaName() },
                { JobArguments.RECONCILIATION_CLOUDWATCH_METRICS_NAMESPACE, validArguments.getReconciliationCloudwatchMetricsNamespace() },
                { JobArguments.RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE, Double.toString(validArguments.getReconciliationCurrentStateCountsToleranceRelativePercentage()) },
                { JobArguments.RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_ABSOLUTE, Long.toString(validArguments.getReconciliationCurrentStateCountsToleranceAbsolute()) },
                { JobArguments.RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE, Double.toString(validArguments.getReconciliationChangeDataCountsToleranceRelativePercentage()) },
                { JobArguments.RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_ABSOLUTE, Long.toString(validArguments.getReconciliationChangeDataCountsToleranceAbsolute()) },
                { JobArguments.SECRET_ID, validArguments.getSecretId() },
                { JobArguments.ADJUST_SPARK_MEMORY, validArguments.adjustSparkMemory() },
                { JobArguments.SPARK_SQL_MAX_RECORDS_PER_FILE, Integer.toString(validArguments.getSparkSqlMaxRecordsPerFile()) },
        }).collect(Collectors.toMap(entry -> entry[0].toString(), entry -> entry[1].toString()));

        assertEquals(testArguments, actualArguments);
    }

    @Test
    public void shouldSetBatchDuration() {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.BATCH_DURATION_SECONDS, "30");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals("30 seconds", jobArguments.getBatchDuration());
    }

    @Test
    public void shouldThrowForNonIntegerBatchDuration() {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.BATCH_DURATION_SECONDS, "30 seconds");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThrows(NumberFormatException.class, jobArguments::getBatchDuration);
    }

    @Test
    public void shouldSetBatchMaxRetries() {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.BATCH_MAX_RETRIES, "20");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(20, jobArguments.getBatchMaxRetries());
    }

    @Test
    public void shouldThrowForNonIntegerBatchMaxRetries() {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.BATCH_MAX_RETRIES, "10 retries");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThrows(NumberFormatException.class, jobArguments::getBatchMaxRetries);
    }

    @ParameterizedTest
    @ValueSource(strings = { "debug", "info", "warn", "error", "DEBUG", "INFO", "WARN", "ERROR" })
    public void shouldSetAllowedLogLevels(String level) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.LOG_LEVEL, level);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(level.toUpperCase(), jobArguments.getLogLevel().toString().toUpperCase());
    }

    @Test
    public void shouldDefaultToWarnForNonAllowedLogLevel() {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.LOG_LEVEL, "some level");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals("WARN", jobArguments.getLogLevel().toString().toUpperCase());
    }

    @ParameterizedTest
    @ValueSource(strings = { "not a boolean", "1", "0", "" })
    public void shouldDefaultToFalseForNonBooleanValueForAddIdleTimeBetweenReads(String input) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.ADD_IDLE_TIME_BETWEEN_READS, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals("false", jobArguments.addIdleTimeBetweenReads());
    }

    @Test
    public void shouldDefaultOperationalDataStoreJdbcBatchSizeTo1000() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(1000L, jobArguments.getOperationalDataStoreJdbcBatchSize());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void shouldConvertValidValueForAddIdleTimeBetweenReadsToBoolean(String input, Boolean expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.ADD_IDLE_TIME_BETWEEN_READS, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected.toString(), jobArguments.addIdleTimeBetweenReads());
    }

    @Test
    public void shouldThrowErrorWhenGivenInvalidIdleTimeBetweenReadsInMillis() {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.IDLE_TIME_BETWEEN_READS_IN_MILLIS, "not a number");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThrows(NumberFormatException.class, jobArguments::getIdleTimeBetweenReadsInMillis);
    }

    @ParameterizedTest
    @CsvSource({ "1, 1", "0, 0", "12345, 12345", "0123, 123" })
    public void shouldSetIdleTimeBetweenReadsInMillis(String input, Integer expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.IDLE_TIME_BETWEEN_READS_IN_MILLIS, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected.toString(), jobArguments.getIdleTimeBetweenReadsInMillis());
    }

    @Test
    public void shouldSetCdcFileGlobPattern() {
        HashMap<String, String> args = cloneTestArguments();
        String expected = "*some-pattern";
        args.put(JobArguments.CDC_FILE_GLOB_PATTERN, expected);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.getCdcFileGlobPattern());
    }

    @Test
    public void shouldDefaultCdcFileGlobPattern() {
        HashMap<String, String> args = cloneTestArguments();
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(JobArguments.CDC_FILE_GLOB_PATTERN_DEFAULT, jobArguments.getCdcFileGlobPattern());
    }

    @Test
    public void shouldSetBatchLoadFileGlobPattern() {
        HashMap<String, String> args = cloneTestArguments();
        String expected = "some-pattern";
        args.put(JobArguments.BATCH_LOAD_FILE_GLOB_PATTERN, expected);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.getBatchLoadFileGlobPattern());
    }

    @Test
    public void shouldDefaultBatchLoadFileGlobPattern() {
        HashMap<String, String> args = cloneTestArguments();
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(JobArguments.BATCH_LOAD_FILE_GLOB_PATTERN_DEFAULT, jobArguments.getBatchLoadFileGlobPattern());
    }

    @ParameterizedTest
    @CsvSource({ "1, 1", "0, 0", "12345, 12345", "0123, 123" })
    public void shouldGetFileTransferRetentionPeriodInDaysWhenNoUnitIsProvided(String input, long expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_AMOUNT, input);
        args.remove(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_UNIT);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Duration.of(expected, ChronoUnit.DAYS), jobArguments.getFileTransferRetentionPeriod());
    }

    @Test
    public void shouldDefaultFileTransferRetentionPeriodToZeroDay() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_AMOUNT);
        args.remove(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_UNIT);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Duration.of(0L, ChronoUnit.DAYS), jobArguments.getFileTransferRetentionPeriod());
    }

    @ParameterizedTest
    @CsvSource({ "minutes", "hours", "days" })
    public void shouldGetFileTransferRetentionPeriodInTheProvidedUnit(String durationUnit) {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_AMOUNT);
        args.put(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_UNIT, durationUnit);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Duration.of(0L, ChronoUnit.valueOf(durationUnit.toUpperCase())), jobArguments.getFileTransferRetentionPeriod());
    }

    @ParameterizedTest
    @CsvSource({
            "nanos",
            "micros",
            "millis",
            "seconds",
            "HalfDays",
            "Weeks",
            "Months",
            "Years",
            "Decades",
            "Centuries",
            "Millennia",
            "Eras",
            "Forever"
    })
    public void shouldFailToGetFileTransferRetentionPeriodWhenGivenUnsupportedUnit(String unsupportedUnit) {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_AMOUNT);
        args.put(JobArguments.FILE_TRANSFER_RETENTION_PERIOD_UNIT, unsupportedUnit);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThrows(IllegalArgumentException.class, jobArguments::getFileTransferRetentionPeriod);
    }

    @ParameterizedTest
    @CsvSource({ "1, 1", "0, 0", "12345, 12345", "0123, 123" })
    public void shouldGetRawFileRetentionPeriodInDaysWhenNoUnitIsProvided(String input, long expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.RAW_FILE_RETENTION_PERIOD_AMOUNT, input);
        args.remove(JobArguments.RAW_FILE_RETENTION_PERIOD_UNIT);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Duration.of(expected, ChronoUnit.DAYS), jobArguments.getRawFileRetentionPeriod());
    }

    @Test
    public void shouldDefaultRawFileRetentionPeriodToTwoDays() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RAW_FILE_RETENTION_PERIOD_AMOUNT);
        args.remove(JobArguments.RAW_FILE_RETENTION_PERIOD_UNIT);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Duration.of(2L, ChronoUnit.DAYS), jobArguments.getRawFileRetentionPeriod());
    }

    @ParameterizedTest
    @CsvSource({ "minutes", "hours", "days" })
    public void shouldGetRawFileRetentionPeriodInTheProvidedUnit(String durationUnit) {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RAW_FILE_RETENTION_PERIOD_AMOUNT);
        args.put(JobArguments.RAW_FILE_RETENTION_PERIOD_UNIT, durationUnit);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Duration.of(DEFAULT_RAW_FILE_RETENTION_PERIOD_AMOUNT, ChronoUnit.valueOf(durationUnit.toUpperCase())), jobArguments.getRawFileRetentionPeriod());
    }

    @ParameterizedTest
    @CsvSource({
            "nanos",
            "micros",
            "millis",
            "seconds",
            "HalfDays",
            "Weeks",
            "Months",
            "Years",
            "Decades",
            "Centuries",
            "Millennia",
            "Eras",
            "Forever"
    })
    public void shouldFailToGetRawFileRetentionPeriodWhenGivenUnsupportedUnit(String unsupportedUnit) {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RAW_FILE_RETENTION_PERIOD_AMOUNT);
        args.put(JobArguments.RAW_FILE_RETENTION_PERIOD_UNIT, unsupportedUnit);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThrows(IllegalArgumentException.class, jobArguments::getRawFileRetentionPeriod);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "dpr-delete-bucket-1,dpr-delete-bucket-2",
            "dpr-delete-bucket-1, dpr-delete-bucket-2",
            "DPR-DELETE-BUCKET-1,dpr-delete-bucket-2",
            "dpr-delete-bucket-1,dpr-delete-bucket-2,",
            "dpr-delete-bucket-1,,dpr-delete-bucket-2,",
            "dpr-delete-bucket-1, ,dpr-delete-bucket-2,",
            ",dpr-delete-bucket-1, ,dpr-delete-bucket-2,"
    })
    public void shouldGetSetOfBucketsToDeleteFilesFrom(String input) {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.FILE_DELETION_BUCKETS, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThat(
                jobArguments.getBucketsToDeleteFilesFrom(),
                containsInAnyOrder(ImmutableSet.of("dpr-delete-bucket-1", "dpr-delete-bucket-2").toArray())
        );
    }

    @Test
    public void shouldThrowAnExceptionWhenSetOfBucketsToDeleteFilesFromIsEmpty() {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.FILE_DELETION_BUCKETS, "");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThrows(IllegalStateException.class, jobArguments::getBucketsToDeleteFilesFrom);
    }

    @Test
    public void shouldReturnCompiledPatternOfAllowedS3FileNameRegexForParquetFiles() {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.ALLOWED_S3_FILE_NAME_REGEX, parquetFileRegex.pattern());
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(parquetFileRegex.pattern(), jobArguments.getAllowedS3FileNameRegex().pattern());
    }

    @Test
    public void shouldReturnCompiledPatternOfAllowedS3FileNameRegexForJsonOrParquetFiles() {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.ALLOWED_S3_FILE_NAME_REGEX, jsonOrParquetFileRegex.pattern());
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(jsonOrParquetFileRegex.pattern(), jobArguments.getAllowedS3FileNameRegex().pattern());
    }

    @Test
    public void shouldMatchAllFilesWhenAllowedS3FileNameRegexNotProvided() {
        HashMap<String, String> args = new HashMap<>();
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(matchAllFiles.pattern(), jobArguments.getAllowedS3FileNameRegex().pattern());
    }

    @Test
    public void shouldDefaultOrchestrationWaitIntervalSecondsWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.ORCHESTRATION_WAIT_INTERVAL_SECONDS);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(10, jobArguments.orchestrationWaitIntervalSeconds());
    }

    @Test
    public void shouldDefaultOrchestrationMaxAttemptsWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.ORCHESTRATION_MAX_ATTEMPTS);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(20, jobArguments.orchestrationMaxAttempts());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "1001"})
    public void shouldReturnErrorWhenMaxObjectsPerPageIsInvalid(String value) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.MAX_S3_PAGE_SIZE, value);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThrows(IllegalArgumentException.class, jobArguments::getMaxObjectsPerPage);
    }

    @Test
    public void shouldDefaultMaxObjectsPerPageWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.MAX_S3_PAGE_SIZE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(1000, jobArguments.getMaxObjectsPerPage());
    }

    @Test
    public void getCdcTriggerIntervalSecondsShouldUseDefaultWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.CDC_TRIGGER_INTERVAL_SECONDS);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(DEFAULT_CDC_TRIGGER_INTERVAL_SECONDS, jobArguments.getCdcTriggerIntervalSeconds());
    }

    @Test
    public void cleanCdcCheckpointShouldDefaultToFalseWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.CLEAN_CDC_CHECKPOINT);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.cleanCdcCheckpoint());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void shouldConvertValidValueForActivateGlueTriggerToBoolean(String input, Boolean expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.ACTIVATE_GLUE_TRIGGER, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.activateGlueTrigger());
    }

    @Test
    public void activateGlueTriggerShouldDefaultToFalseWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.ACTIVATE_GLUE_TRIGGER);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.activateGlueTrigger());
    }

    @Test
    public void streamingJobMaxFilePerTriggerShouldUseDefaultWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.STREAMING_JOB_MAX_FILES_PER_TRIGGER);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER, jobArguments.streamingJobMaxFilePerTrigger());
    }

    @Test
    public void shouldUseNowAsCheckpointForReloadJobShouldDefaultToFalseWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RELOAD_JOB_USE_NOW_AS_CHECKPOINT);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.shouldUseNowAsCheckpointForReloadJob());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void shouldUseNowAsCheckpointForReloadJobShouldUseProvidedBooleanValue(String input, Boolean expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.RELOAD_JOB_USE_NOW_AS_CHECKPOINT, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.shouldUseNowAsCheckpointForReloadJob());
    }

    @Test
    public void operationalDataStoreWriteEnabledShouldDefaultToFalseWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.OPERATIONAL_DATA_STORE_WRITE_ENABLED);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.isOperationalDataStoreWriteEnabled());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void operationalDataStoreWriteEnabledShouldUseProvidedBooleanValue(String input, Boolean expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.OPERATIONAL_DATA_STORE_WRITE_ENABLED, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.isOperationalDataStoreWriteEnabled());
    }

    @Test
    public void operationalDataStoreLoadingSchemaNameShouldDefaultWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.OPERATIONAL_DATA_STORE_LOADING_SCHEMA_NAME);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals("loading", jobArguments.getOperationalDataStoreLoadingSchemaName());
    }

    @Test
    public void defaultBroadcastTimeoutSecondsWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.SPARK_BROADCAST_TIMEOUT_SECONDS);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(DEFAULT_SPARK_BROADCAST_TIMEOUT_SECONDS, jobArguments.getBroadcastTimeoutSeconds());
    }

    @Test
    public void defaultSparkSqlMaxRecordsPerFileWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.SPARK_SQL_MAX_RECORDS_PER_FILE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(DEFAULT_SPARK_SQL_MAX_RECORDS_PER_FILE, jobArguments.getSparkSqlMaxRecordsPerFile());
    }

    @Test
    public void disableAutoBroadcastJoinThresholdShouldDefaultToFalseWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.DISABLE_AUTO_BROADCAST_JOIN_THRESHOLD);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.disableAutoBroadcastJoinThreshold());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void disableAutoBroadcastJoinThresholdShouldUseProvidedBooleanValue(String input, Boolean expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.DISABLE_AUTO_BROADCAST_JOIN_THRESHOLD, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.disableAutoBroadcastJoinThreshold());
    }

    @Test
    public void enableStreamingSourceArchivingShouldDefaultToFalseWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.ENABLE_STREAMING_SOURCE_ARCHIVING);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.enableStreamingSourceArchiving());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void enableStreamingSourceArchivingShouldUseProvidedBooleanValue(String input, Boolean expected) {
        HashMap<String, String> args = cloneTestArguments();
        args.put(JobArguments.ENABLE_STREAMING_SOURCE_ARCHIVING, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.enableStreamingSourceArchiving());
    }

    @Test
    public void getOptionalConfigKeyShouldDefaultToEmptyOptionWhenNoConfigKeyIsProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.CONFIG_KEY);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Optional.empty(), jobArguments.getOptionalConfigKey());
    }

    @Test
    public void getOptionalConfigKeyShouldReturnOptionalWithConfigKeyPresentWhenConfigKeyIsProvided() {
        HashMap<String, String> args = cloneTestArguments();
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(Optional.of("test-config"), jobArguments.getOptionalConfigKey());
    }

    @Test
    public void shouldReturnCorrectValuesInGetConfig() {
        Map<String, String> actualArguments = validArguments.getConfig();
        assertEquals(testArguments, actualArguments);
    }

    @Test
    public void shouldNotAllowGetConfigMapToBeModified() {
        Map<String, String> arguments = validArguments.getConfig();
        assertThrows(UnsupportedOperationException.class, () -> arguments.put(JobArguments.BATCH_MAX_RETRIES, "6"));
    }

    @Test
    public void shouldThrowAnExceptionWhenAMissingArgumentIsRequested() {
        assertThrows(IllegalStateException.class, emptyArguments::getAwsRegion);
    }

    @Test
    public void shouldGetCurrentStateCountsReconciliationCheckToRun() {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.RECONCILIATION_CHECKS_TO_RUN, "current_state_counts");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(ImmutableSet.of(CURRENT_STATE_COUNTS), jobArguments.getReconciliationChecksToRun());
    }

    @Test
    public void shouldGetChangeDataCountsReconciliationCheckToRun() {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.RECONCILIATION_CHECKS_TO_RUN, "change_data_counts");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(ImmutableSet.of(CHANGE_DATA_COUNTS), jobArguments.getReconciliationChecksToRun());
    }

    @Test
    public void shouldGetPrimaryKeyReconciliationCheckToRun() {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.RECONCILIATION_CHECKS_TO_RUN, "primary_key_reconciliation");
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(ImmutableSet.of(PRIMARY_KEY_RECONCILIATION), jobArguments.getReconciliationChecksToRun());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "current_state_counts,change_data_counts",
            "current_state_counts, change_data_counts",
            "CURRENT_STATE_COUNTS,change_data_counts",
            "current_state_counts,change_data_counts,",
            "current_state_counts,,change_data_counts,",
            "current_state_counts, ,change_data_counts,",
            ",current_state_counts, ,change_data_counts,",
            "current_state_counts,current_state_counts,change_data_counts,change_data_counts"
    })
    public void shouldGetMultipleReconciliationChecksToRun(String input) {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.RECONCILIATION_CHECKS_TO_RUN, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertThat(
                jobArguments.getReconciliationChecksToRun(),
                containsInAnyOrder(CURRENT_STATE_COUNTS, CHANGE_DATA_COUNTS)
        );
    }

    @Test
    public void getReconciliationChecksToRunShouldDefaultWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RECONCILIATION_CHECKS_TO_RUN);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(RECONCILIATION_CHECKS_TO_RUN_DEFAULT, jobArguments.getReconciliationChecksToRun());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void shouldReconciliationDataSourceTableNamesBeUpperCaseShouldParseInput(String input, boolean expected) {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.RECONCILIATION_DATASOURCE_SHOULD_UPPERCASE_TABLENAMES, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.shouldReconciliationDataSourceTableNamesBeUpperCase());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void shouldReconciliationFailJobIfChecksFailShouldParseInput(String input, boolean expected) {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.RECONCILIATION_FAIL_JOB_IF_CHECKS_FAILS, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.shouldReconciliationFailJobIfChecksFail());
    }

    @Test
    public void shouldReconciliationFailJobIfChecksFailShouldDefaultToFalseWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RECONCILIATION_FAIL_JOB_IF_CHECKS_FAILS);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.shouldReconciliationFailJobIfChecksFail());
    }

    @ParameterizedTest
    @CsvSource({ "true, true", "false, false", "True, true", "False, false" })
    public void shouldReportReconciliationResultsToCloudwatchShouldParseInput(String input, boolean expected) {
        HashMap<String, String> args = new HashMap<>();
        args.put(JobArguments.RECONCILIATION_REPORT_RESULTS_TO_CLOUDWATCH, input);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(expected, jobArguments.shouldReportReconciliationResultsToCloudwatch());
    }

    @Test
    public void shouldReportReconciliationResultsToCloudwatchShouldDefaultToFalseWhenMissing() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RECONCILIATION_REPORT_RESULTS_TO_CLOUDWATCH);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.shouldReportReconciliationResultsToCloudwatch());
    }

    @Test
    void getReconciliationChangeDataCountsToleranceRelativePercentageShouldDefaultToZero() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(0.0, jobArguments.getReconciliationChangeDataCountsToleranceRelativePercentage());
    }

    @Test
    void getReconciliationChangeDataCountsToleranceAbsoluteShouldDefaultToZero() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_ABSOLUTE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(0L, jobArguments.getReconciliationChangeDataCountsToleranceAbsolute());
    }

    @Test
    void getReconciliationCurrentStateCountsToleranceRelativePercentageShouldDefaultToZero() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(0.0, jobArguments.getReconciliationCurrentStateCountsToleranceRelativePercentage());
    }

    @Test
    void getReconciliationCurrentStateCountsToleranceAbsoluteShouldDefaultToZero() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_ABSOLUTE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(0L, jobArguments.getReconciliationCurrentStateCountsToleranceAbsolute());
    }

    @Test
    void adjustSparkMemoryShouldDefaultToFalse() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.ADJUST_SPARK_MEMORY);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertFalse(jobArguments.adjustSparkMemory());
    }

    @Test
    void getTestDataTableNameShouldUseDefaultWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.TEST_DATA_TABLE_NAME);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals("test_table", jobArguments.getTestDataTableName());
    }

    @Test
    void getTestDataBatchSizeShouldUseDefaultWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.TEST_DATA_BATCH_SIZE);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(5, jobArguments.getTestDataBatchSize());
    }

    @Test
    void getTestDataParallelismShouldUseDefaultWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.TEST_DATA_PARALLELISM);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(1, jobArguments.getTestDataParallelism());
    }

    @Test
    void getTestDataInterBatchDelayMillisShouldUseDefaultWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.TEST_DATA_INTER_BATCH_DELAY);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(1000, jobArguments.getTestDataInterBatchDelayMillis());
    }

    @Test
    void getRunDurationMillisShouldUseDefaultWhenNotProvided() {
        HashMap<String, String> args = cloneTestArguments();
        args.remove(JobArguments.TEST_DATA_RUN_DURATION_MILLIS);
        JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
        assertEquals(3600000, jobArguments.getRunDurationMillis());
    }

    private static ApplicationContext givenAContextWithArguments(Map<String, String> m) {
        val mockContext = mock(ApplicationContext.class);
        val mockEnvironment = mock(Environment.class);
        val mockCommandLinePropertySource = mock(CommandLinePropertySource.class);

        when(mockCommandLinePropertySource.getName()).thenReturn(CommandLinePropertySource.NAME);
        when(mockCommandLinePropertySource.asMap()).thenReturn(convertArgumentValuesToObject(m));

        when(mockEnvironment.getPropertySources()).thenReturn(Collections.singleton(mockCommandLinePropertySource));

        when(mockContext.getEnvironment()).thenReturn(mockEnvironment);

        return mockContext;
    }

    private static Map<String, Object> convertArgumentValuesToObject(Map<String, String> m) {
        return m.entrySet()
                .stream()
                .map(e -> new AbstractMap.SimpleEntry<>(e.getKey(), e.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static ApplicationContext givenAContextWithNoArguments() {
        val mockContext = mock(ApplicationContext.class);
        val mockEnvironment = mock(Environment.class);

        // If no command line arguments are provided no CommandLinePropertySource will be created.
        when(mockEnvironment.getPropertySources()).thenReturn(Collections.emptySet());
        when(mockContext.getEnvironment()).thenReturn(mockEnvironment);

        return mockContext;
    }

    @SuppressWarnings("unchecked")
    private static HashMap<String, String> cloneTestArguments() {
        return (HashMap<String, String>)((HashMap<String, String>) testArguments).clone();
    }
}
