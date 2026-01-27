package uk.gov.justice.digital.config;

import com.google.common.collect.ImmutableSet;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.CommandLinePropertySource;
import io.micronaut.context.env.MapPropertySource;
import io.micronaut.context.env.PropertySource;
import io.micronaut.logging.LogLevel;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.AbstractMap.SimpleEntry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.client.s3.S3ObjectClient.DELIMITER;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CHANGE_DATA_COUNTS;
import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck.CURRENT_STATE_COUNTS;

/**
 * Class that defines and provides access to the job arguments that we support.
 * Arguments are taken from the values parsed by the CommandLinePropertySource.
 */
@Singleton
public class JobArguments {

    private static final Logger logger = LoggerFactory.getLogger(JobArguments.class);

    public static final String JOBS_S3_BUCKET = "dpr.jobs.s3.bucket";
    public static final String CONFIG_S3_BUCKET = "dpr.config.s3.bucket";
    public static final String READ_CONFIG_FROM_S3 = "dpr.read.config.from.s3";
    public static final String CONFIG_KEY = "dpr.config.key";
    public static final String AWS_DYNAMODB_ENDPOINT_URL = "dpr.aws.dynamodb.endpointUrl";
    public static final String AWS_REGION = "dpr.aws.region";
    public static final String LOG_LEVEL = "dpr.log.level";
    public static final String CONTRACT_REGISTRY_NAME = "dpr.contract.registryName";
    public static final String SCHEMA_CACHE_MAX_SIZE = "dpr.schema.cache.max.size";
    public static final String SCHEMA_CACHE_EXPIRY_IN_MINUTES = "dpr.schema.cache.expiry.minutes";
    public static final String CURATED_S3_PATH = "dpr.curated.s3.path";
    public static final String PRISONS_DATA_SWITCH_TARGET_S3_PATH = "dpr.prisons.data.switch.target.s3.path";
    // Raw files that have been processed by the streaming job are moved to this location before being archived.
    // Spark structured streaming only allows moving processed files to a location outside the source to avoid recycling the files.
    // It should be noted that the processed files can only be moved to a location within the same s3 bucket.
    public static final String PROCESSED_RAW_FILES_PATH = "dpr.processed.raw.files.path";
    public static final String ENABLE_STREAMING_SOURCE_ARCHIVING = "dpr.enable.streaming.source.archiving";
    public static final String DOMAIN_CATALOG_DATABASE_NAME = "dpr.domain.catalog.db";
    public static final String DOMAIN_NAME = "dpr.domain.name";
    public static final String DOMAIN_OPERATION = "dpr.domain.operation";
    public static final String DOMAIN_REGISTRY = "dpr.domain.registry";
    public static final String DOMAIN_TARGET_PATH = "dpr.domain.target.path";
    public static final String DOMAIN_TABLE_NAME = "dpr.domain.table.name";
    public static final String BATCH_DURATION_SECONDS = "dpr.batchDurationSeconds";
    // dpr.add.idle.time.between.reads defaults to false thereby causing IdleTimeBetweenReadsInMs to default to 0 ms
    public static final String ADD_IDLE_TIME_BETWEEN_READS = "dpr.add.idle.time.between.reads";
    // The provided value for dpr.idle.time.between.reads.millis is only applied when dpr.add.idle.time.between.reads is true
    public static final String IDLE_TIME_BETWEEN_READS_IN_MILLIS = "dpr.idle.time.between.reads.millis";
    public static final String RAW_S3_PATH = "dpr.raw.s3.path";
    public static final String RAW_ARCHIVE_S3_PATH = "dpr.raw.archive.s3.path";
    public static final String STRUCTURED_S3_PATH = "dpr.structured.s3.path";
    public static final String VIOLATIONS_S3_PATH = "dpr.violations.s3.path";
    public static final String TEMP_RELOAD_S3_PATH = "dpr.temp.reload.s3.path";
    public static final String TEMP_RELOAD_OUTPUT_FOLDER = "dpr.temp.reload.output.folder";
    public static final String RAW_ARCHIVE_DATABASE = "dpr.raw_archive.database";
    public static final String STRUCTURED_DATABASE = "dpr.structured.database";
    public static final String CURATED_DATABASE = "dpr.curated.database";
    public static final String PRISONS_DATABASE = "dpr.prisons.database";
    public static final String MAINTENANCE_TABLES_ROOT_PATH = "dpr.maintenance.root.path";
    public static final String MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH = "dpr.maintenance.listtable.recurseMaxDepth";
    // The Domain layer only has a depth of 2, with tables nested under domains
    // e.g. s3://dpr-domain-preproduction/establishment/living_unit/
    public static final int MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH_DEFAULT = 2;
    public static final String CHECKPOINT_LOCATION = "checkpoint.location";
    public static final String BATCH_MAX_RETRIES = "dpr.batch.max.retries";
    public static final int BATCH_MAX_RETRIES_DEFAULT = 3;
    public static final String DATA_STORAGE_RETRY_MAX_ATTEMPTS = "dpr.datastorage.retry.maxAttempts";
    // You can turn off retries by setting max attempts to 1
    public static final int DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT = 1;
    public static final long SCHEMA_CACHE_MAX_SIZE_DEFAULT = 2000L;
    public static final long SCHEMA_CACHE_EXPIRY_IN_MINUTES_DEFAULT = 120L;

    public static final String DATA_STORAGE_RETRY_MIN_WAIT_MILLIS = "dpr.datastorage.retry.minWaitMillis";
    public static final long DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT = 100L;

    public static final String DATA_STORAGE_RETRY_MAX_WAIT_MILLIS = "dpr.datastorage.retry.maxWaitMillis";
    public static final long DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT = 10000L;

    public static final String DATA_STORAGE_RETRY_JITTER_FACTOR = "dpr.datastorage.retry.jitterFactor";
    public static final double DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT = 0.25;

    public static final String CDC_FILE_GLOB_PATTERN = "dpr.cdc.fileglobpattern";
    // You might set this to '*-*.parquet' to only process CDC files or '*.parquet' to process load and CDC files
    public static final String CDC_FILE_GLOB_PATTERN_DEFAULT = "*-*.parquet";

    public static final String BATCH_LOAD_FILE_GLOB_PATTERN = "dpr.batch.load.fileglobpattern";
    public static final String BATCH_LOAD_FILE_GLOB_PATTERN_DEFAULT = "LOAD*parquet";
    public static final String FILE_TRANSFER_SOURCE_BUCKET_NAME = "dpr.file.transfer.source.bucket";
    public static final String FILE_SOURCE_PREFIX = "dpr.file.source.prefix";
    public static final String FILE_TRANSFER_DESTINATION_BUCKET_NAME = "dpr.file.transfer.destination.bucket";
    public static final String FILE_TRANSFER_DESTINATION_PREFIX = "dpr.file.transfer.destination.prefix";
    public static final String FILE_TRANSFER_RETENTION_PERIOD_AMOUNT = "dpr.file.transfer.retention.period.amount";
    static final Long DEFAULT_FILE_TRANSFER_RETENTION_PERIOD_AMOUNT = 0L;

    public static final String FILE_TRANSFER_RETENTION_PERIOD_UNIT = "dpr.file.transfer.retention.period.unit";
    static final String DEFAULT_FILE_TRANSFER_RETENTION_PERIOD_UNIT = "days";

    public static final String FILE_TRANSFER_DELETE_COPIED_FILES_FLAG = "dpr.file.transfer.delete.copied.files";
    public static final String FILE_TRANSFER_USE_DEFAULT_PARALLELISM = "dpr.file.transfer.use.default.parallelism";
    public static final String FILE_TRANSFER_PARALLELISM = "dpr.file.transfer.parallelism";

    // A comma separated list of buckets to delete files from
    static final String FILE_DELETION_BUCKETS = "dpr.file.deletion.buckets";
    // A comma separated list of s3 file extensions. The wildcard '*' includes all extensions.
    static final String ALLOWED_S3_FILE_NAME_REGEX = "dpr.allowed.s3.file.regex";
    public static final String DEFAULT_FILE_NAME_REGEX = ".+";
    static final String ORCHESTRATION_WAIT_INTERVAL_SECONDS = "dpr.orchestration.wait.interval.seconds";
    static final int DEFAULT_ORCHESTRATION_WAIT_INTERVAL_SECONDS = 40;
    static final String ORCHESTRATION_MAX_ATTEMPTS = "dpr.orchestration.max.attempts";
    static final int DEFAULT_ORCHESTRATION_MAX_ATTEMPTS = 40;
    static final String STOP_GLUE_INSTANCE_JOB_NAME = "dpr.stop.glue.instance.job.name";
    static final String DMS_REPLICATION_TASK_ID = "dpr.dms.replication.task.id";
    static final String CDC_DMS_REPLICATION_TASK_ID = "dpr.cdc.dms.replication.task.id";
    static final String RELOAD_JOB_USE_NOW_AS_CHECKPOINT = "dpr.reload.checkpoint.use.now";
    static final String MAX_S3_PAGE_SIZE = "dpr.s3.max.page.size";
    static final Integer DEFAULT_MAX_S3_PAGE_SIZE = 1000;
    static final String CLEAN_CDC_CHECKPOINT = "dpr.clean.cdc.checkpoint";
    static final String CDC_TRIGGER_INTERVAL_SECONDS = "dpr.cdc.trigger.interval.seconds";
    static final long DEFAULT_CDC_TRIGGER_INTERVAL_SECONDS = 60;
    static final String SPARK_BROADCAST_TIMEOUT_SECONDS = "dpr.spark.broadcast.timeout.seconds";
    public static final Integer DEFAULT_SPARK_BROADCAST_TIMEOUT_SECONDS = 300;
    // For maxrecordsperfile 100,000 is a good first guess if you're not sure about input record sizes.
    // This should give you roughly the following output file sizes:
    // ~50MB for ~0.5KB records
    // ~200MB for ~2KB records
    // ~500MB for ~5KB records
    static final String SPARK_SQL_MAX_RECORDS_PER_FILE = "dpr.spark.sql.maxrecordsperfile";
    public static final int DEFAULT_SPARK_SQL_MAX_RECORDS_PER_FILE = 0;
    static final String DISABLE_AUTO_BROADCAST_JOIN_THRESHOLD = "dpr.disable.auto.broadcast.join.threshold";
    static final String GLUE_TRIGGER_NAME = "dpr.glue.trigger.name";
    static final String ACTIVATE_GLUE_TRIGGER = "dpr.glue.trigger.activate";
    static final String STREAMING_JOB_MAX_FILES_PER_TRIGGER = "dpr.streaming.job.max.files.per.trigger";
    public static final long STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER = 1000;
    static final String OPERATIONAL_DATA_STORE_WRITE_ENABLED = "dpr.operational.data.store.write.enabled";
    static final String OPERATIONAL_DATA_STORE_GLUE_CONNECTION_NAME = "dpr.operational.data.store.glue.connection.name";
    static final String OPERATIONAL_DATA_STORE_LOADING_SCHEMA_NAME = "dpr.operational.data.store.loading.schema.name";
    static final String OPERATIONAL_DATA_STORE_LOADING_SCHEMA_NAME_DEFAULT = "loading";
    static final String OPERATIONAL_DATA_STORE_TABLES_TO_WRITE_TABLE_NAME = "dpr.operational.data.store.tables.to.write.table.name";
    static final String OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE = "dpr.operational.data.store.jdbc.batch.size";
    public static final long OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT = 1000;
    static final String RECONCILIATION_DATASOURCE_SOURCE_SCHEMA_NAME = "dpr.reconciliation.datasource.source.schema.name";
    static final String RECONCILIATION_DATASOURCE_GLUE_CONNECTION_NAME = "dpr.reconciliation.datasource.glue.connection.name";
    static final String RECONCILIATION_DATASOURCE_SHOULD_UPPERCASE_TABLENAMES = "dpr.reconciliation.datasource.should.uppercase.tablenames";
    static final String RECONCILIATION_CHECKS_TO_RUN = "dpr.reconciliation.checks.to.run";
    static final Set<ReconciliationCheck> RECONCILIATION_CHECKS_TO_RUN_DEFAULT = new HashSet<>(Arrays.asList(CURRENT_STATE_COUNTS, CHANGE_DATA_COUNTS));
    static final String RECONCILIATION_FAIL_JOB_IF_CHECKS_FAILS = "dpr.reconciliation.fail.job.if.checks.fail";
    static final String RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE = "dpr.reconciliation.changedatacounts.tolerance.relative.percentage";
    static final double RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE_DEFAULT = 0.0;
    static final String RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_ABSOLUTE = "dpr.reconciliation.changedatacounts.tolerance.absolute";
    static final long RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_ABSOLUTE_DEFAULT = 0L;
    static final String RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE = "dpr.reconciliation.currentstatecounts.tolerance.relative.percentage";
    static final double RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE_DEFAULT = 0.0;
    static final String RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_ABSOLUTE = "dpr.reconciliation.currentstatecounts.tolerance.absolute";
    static final long RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_ABSOLUTE_DEFAULT = 0L;
    public static final String RAW_FILE_RETENTION_PERIOD_AMOUNT = "dpr.raw.file.retention.period.amount";
    public static final Long DEFAULT_RAW_FILE_RETENTION_PERIOD_AMOUNT = 2L;
    public static final String RAW_FILE_RETENTION_PERIOD_UNIT = "dpr.raw.file.retention.period.unit";
    static final String DEFAULT_RAW_FILE_RETENTION_PERIOD_UNIT = "days";
    static final String SECRET_ID = "dpr.test.database.secret.id";
    static final String TEST_DATA_BATCH_SIZE = "dpr.test.data.batch.size";
    static final int DEFAULT_TEST_DATA_BATCH_SIZE = 5;
    static final String TEST_DATA_PARALLELISM = "dpr.test.data.parallelism";
    static final int DEFAULT_TEST_DATA_PARALLELISM = 1;
    static final String TEST_DATA_INTER_BATCH_DELAY = "dpr.test.data.inter.batch.delay.millis";
    static final long DEFAULT_TEST_DATA_INTER_BATCH_DELAY_MILLIS = 1000;
    static final String TEST_DATA_TABLE_NAME = "dpr.test.data.table.name";
    static final String DEFAULT_TEST_DATA_TABLE_NAME = "test_table";
    static final String TEST_DATA_RUN_DURATION_MILLIS = "dpr.test.data.run.duration.millis";
    static final long DEFAULT_TEST_DATA_RUN_DURATION_MILLIS = 3600000; // 1 hour
    public static final String ADJUST_SPARK_MEMORY = "dpr.adjust.spark.memory";
    public static final String REPORT_METRICS_TO_CLOUDWATCH = "dpr.report.metrics.to.cloudwatch";
    static final String CLOUDWATCH_METRICS_NAMESPACE = "dpr.cloudwatch.metrics.namespace";
    static final String CLOUDWATCH_METRICS_REPORTING_PERIOD_SECONDS = "dpr.cloudwatch.metrics.reporting.period.seconds";
    static final long CLOUDWATCH_METRICS_REPORTING_PERIOD_SECONDS_DEFAULT = 30L;
    private final Map<String, String> config;

    @Inject
    public JobArguments(ApplicationContext context) {
        this(getCommandLineArgumentsFromContext(context));
    }

    public JobArguments(Map<String, String> config) {
        this.config = config.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        logger.info("Job initialised with parameters: {}", config);
    }

    public LogLevel getLogLevel() {
        String logLevel = getArgument(LOG_LEVEL).toLowerCase();
        switch (logLevel) {
            case "debug":
                return LogLevel.DEBUG;
            case "info":
                return LogLevel.INFO;
            case "warn":
                return LogLevel.WARN;
            case "error":
                return LogLevel.ERROR;
            default:
                logger.warn("Invalid log level {} provided. Defaulting to WARN", logLevel);
                return LogLevel.WARN;
        }
    }

    public Map<String, String> getConfig() {
        return Collections.unmodifiableMap(this.config);
    }

    public String getAwsRegion() {
        return getArgument(AWS_REGION);
    }

    public String getAwsDynamoDBEndpointUrl() {
        return getArgument(AWS_DYNAMODB_ENDPOINT_URL);
    }

    public String getBatchDuration() {
        int numSeconds = Integer.parseInt(getArgument(BATCH_DURATION_SECONDS));
        return numSeconds + " seconds";
    }

    public String addIdleTimeBetweenReads() {
        return String.valueOf(
                Optional.of(config.get(ADD_IDLE_TIME_BETWEEN_READS).toLowerCase())
                        .map(Boolean::parseBoolean)
                        .orElse(false));
    }

    public String getIdleTimeBetweenReadsInMillis() {
        if (!Boolean.parseBoolean(addIdleTimeBetweenReads())) {
            logger.warn(
                    "Argument {} will not be applied when {} is omitted or set to false",
                    IDLE_TIME_BETWEEN_READS_IN_MILLIS,
                    ADD_IDLE_TIME_BETWEEN_READS
            );
        }

        return String.valueOf(Integer.parseInt(getArgument(IDLE_TIME_BETWEEN_READS_IN_MILLIS)));
    }

    public String getRawS3Path() {
        return getArgument(RAW_S3_PATH);
    }

    public String getRawArchiveS3Path() {
        return getArgument(RAW_ARCHIVE_S3_PATH);
    }

    public String getStructuredS3Path() {
        return getArgument(STRUCTURED_S3_PATH);
    }

    public String getViolationsS3Path() {
        return getArgument(VIOLATIONS_S3_PATH);
    }

    public String getTempReloadS3Path() {
        return getArgument(TEMP_RELOAD_S3_PATH);
    }

    public String getTempReloadOutputFolder() {
        return getArgument(TEMP_RELOAD_OUTPUT_FOLDER);
    }

    public String getRawArchiveDatabase() {
        return getArgument(RAW_ARCHIVE_DATABASE);
    }

    public String getStructuredDatabase() {
        return getArgument(STRUCTURED_DATABASE);
    }

    public String getCuratedDatabase() {
        return getArgument(CURATED_DATABASE);
    }

    public String getPrisonsDatabase() {
        return getArgument(PRISONS_DATABASE);
    }

    public String getCuratedS3Path() {
        return getArgument(CURATED_S3_PATH);
    }

    public String getPrisonsDataSwitchTargetS3Path() {
        return getArgument(PRISONS_DATA_SWITCH_TARGET_S3_PATH);
    }

    public boolean enableStreamingSourceArchiving() {
        return getArgument(ENABLE_STREAMING_SOURCE_ARCHIVING, false);
    }

    public String getProcessedRawFilesPath() {
        return removeLeadingAndTrailingSlashes(getArgument(PROCESSED_RAW_FILES_PATH));
    }

    public String getDomainTargetPath() {
        return getArgument(DOMAIN_TARGET_PATH);
    }

    public String getDomainName() {
        return getArgument(DOMAIN_NAME);
    }

    public String getDomainTableName() {
        return getArgument(DOMAIN_TABLE_NAME);
    }

    public String getDomainRegistry() {
        return getArgument(DOMAIN_REGISTRY);
    }

    public String getDomainOperation() {
        return getArgument(DOMAIN_OPERATION);
    }

    public String getDomainCatalogDatabaseName() {
        return getArgument(DOMAIN_CATALOG_DATABASE_NAME);
    }

    public String getContractRegistryName() {
        return getArgument(CONTRACT_REGISTRY_NAME);
    }

    public Long getSchemaCacheMaxSize() {
        return getArgument(SCHEMA_CACHE_MAX_SIZE, SCHEMA_CACHE_MAX_SIZE_DEFAULT);
    }

    public Long getSchemaCacheExpiryInMinutes() {
        return getArgument(SCHEMA_CACHE_EXPIRY_IN_MINUTES, SCHEMA_CACHE_EXPIRY_IN_MINUTES_DEFAULT);
    }

    public String getConfigS3Bucket() {
        return getArgument(CONFIG_S3_BUCKET);
    }

    public boolean readConfigFromS3() {
        return getArgument(READ_CONFIG_FROM_S3, false);
    }

    public String getConfigKey() {
        return getArgument(CONFIG_KEY);
    }

    public Optional<String> getOptionalConfigKey() {
        return Optional.ofNullable(config.get(CONFIG_KEY));
    }

    public String getJobsS3Bucket() {
        return getArgument(JOBS_S3_BUCKET);
    }

    public String getMaintenanceTablesRootPath() {
        return getArgument(MAINTENANCE_TABLES_ROOT_PATH);
    }

    public int getMaintenanceListTableRecurseMaxDepth() {
        return getArgument(MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH, MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH_DEFAULT);
    }

    public String getCheckpointLocation() {
        return getArgument(CHECKPOINT_LOCATION);
    }

    public int getBatchMaxRetries() {
        return getArgument(BATCH_MAX_RETRIES, BATCH_MAX_RETRIES_DEFAULT);
    }

    public int getDataStorageRetryMaxAttempts() {
        return getArgument(DATA_STORAGE_RETRY_MAX_ATTEMPTS, DATA_STORAGE_RETRY_MAX_ATTEMPTS_DEFAULT);
    }

    public long getDataStorageRetryMinWaitMillis() {
        return getArgument(DATA_STORAGE_RETRY_MIN_WAIT_MILLIS, DATA_STORAGE_RETRY_MIN_WAIT_MILLIS_DEFAULT);
    }

    public long getDataStorageRetryMaxWaitMillis() {
        return getArgument(DATA_STORAGE_RETRY_MAX_WAIT_MILLIS, DATA_STORAGE_RETRY_MAX_WAIT_MILLIS_DEFAULT);
    }

    public double getDataStorageRetryJitterFactor() {
        return getArgument(DATA_STORAGE_RETRY_JITTER_FACTOR, DATA_STORAGE_RETRY_JITTER_FACTOR_DEFAULT);
    }

    public String getCdcFileGlobPattern() {
        return getArgument(CDC_FILE_GLOB_PATTERN, CDC_FILE_GLOB_PATTERN_DEFAULT);
    }

    public String getBatchLoadFileGlobPattern() {
        return getArgument(BATCH_LOAD_FILE_GLOB_PATTERN, BATCH_LOAD_FILE_GLOB_PATTERN_DEFAULT);
    }

    public String getTransferSourceBucket() {
        return getArgument(FILE_TRANSFER_SOURCE_BUCKET_NAME);
    }

    public String getSourcePrefix() {
        return removeLeadingAndTrailingSlashes(getArgument(FILE_SOURCE_PREFIX, ""));
    }

    public String getTransferDestinationBucket() {
        return getArgument(FILE_TRANSFER_DESTINATION_BUCKET_NAME);
    }

    public String getTransferDestinationPrefix() {
        return removeLeadingAndTrailingSlashes(getArgument(FILE_TRANSFER_DESTINATION_PREFIX, ""));
    }

    public Duration getFileTransferRetentionPeriod() {
        long retentionAmount = getArgument(FILE_TRANSFER_RETENTION_PERIOD_AMOUNT, DEFAULT_FILE_TRANSFER_RETENTION_PERIOD_AMOUNT);
        String retentionUnit = getArgument(FILE_TRANSFER_RETENTION_PERIOD_UNIT, DEFAULT_FILE_TRANSFER_RETENTION_PERIOD_UNIT);

        return convertToPeriod(retentionUnit, retentionAmount, FILE_TRANSFER_RETENTION_PERIOD_UNIT);
    }

    public boolean fileTransferUseDefaultParallelism() {
        return getArgument(FILE_TRANSFER_USE_DEFAULT_PARALLELISM, true);
    }

    public Integer getFileTransferParallelism() {
        if (fileTransferUseDefaultParallelism()) {
            throw new IllegalArgumentException(FILE_TRANSFER_PARALLELISM + " can only be set when " + FILE_TRANSFER_USE_DEFAULT_PARALLELISM + " is false");
        } else {
            int fileTransferParallelism = Integer.parseInt(getArgument(FILE_TRANSFER_PARALLELISM));
            if (fileTransferParallelism < 1) {
                throw new IllegalArgumentException(FILE_TRANSFER_PARALLELISM + " must must be >= 1");
            } else {
                return fileTransferParallelism;
            }
        }
    }

    public Duration getRawFileRetentionPeriod() {
        long retentionAmount = getArgument(RAW_FILE_RETENTION_PERIOD_AMOUNT, DEFAULT_RAW_FILE_RETENTION_PERIOD_AMOUNT);
        String retentionUnit = getArgument(RAW_FILE_RETENTION_PERIOD_UNIT, DEFAULT_RAW_FILE_RETENTION_PERIOD_UNIT);

        return convertToPeriod(retentionUnit, retentionAmount, RAW_FILE_RETENTION_PERIOD_UNIT);
    }

    public boolean getFileTransferDeleteCopiedFilesFlag() {
        return getArgument(FILE_TRANSFER_DELETE_COPIED_FILES_FLAG, false);
    }

    public ImmutableSet<String> getBucketsToDeleteFilesFrom() {
        Set<String> buckets = Arrays.stream(getArgument(FILE_DELETION_BUCKETS).toLowerCase().split(","))
                .map(String::trim)
                .filter(item -> !item.isEmpty())
                .collect(Collectors.toSet());
        if (buckets.isEmpty()) throw new IllegalStateException("Argument " + FILE_DELETION_BUCKETS + " evaluated to empty set");
        return ImmutableSet.copyOf(buckets);
    }

    public Pattern getAllowedS3FileNameRegex() {
        return Pattern.compile(getArgument(ALLOWED_S3_FILE_NAME_REGEX, DEFAULT_FILE_NAME_REGEX));
    }

    public String getStopGlueInstanceJobName() {
        return getArgument(STOP_GLUE_INSTANCE_JOB_NAME);
    }

    public String getDmsTaskId() {
        return getArgument(DMS_REPLICATION_TASK_ID);
    }

    public String getCdcDmsTaskId() {
        return getArgument(CDC_DMS_REPLICATION_TASK_ID);
    }

    public boolean shouldUseNowAsCheckpointForReloadJob() {
        return getArgument(RELOAD_JOB_USE_NOW_AS_CHECKPOINT, false);
    }

    public boolean isOperationalDataStoreWriteEnabled() {
        return getArgument(OPERATIONAL_DATA_STORE_WRITE_ENABLED, false);
    }

    public String getOperationalDataStoreGlueConnectionName() {
        return getArgument(OPERATIONAL_DATA_STORE_GLUE_CONNECTION_NAME);
    }

    public String getOperationalDataStoreLoadingSchemaName() {
        return getArgument(OPERATIONAL_DATA_STORE_LOADING_SCHEMA_NAME, OPERATIONAL_DATA_STORE_LOADING_SCHEMA_NAME_DEFAULT);
    }

    public String getOperationalDataStoreTablesToWriteTableName() {
        return getArgument(OPERATIONAL_DATA_STORE_TABLES_TO_WRITE_TABLE_NAME);
    }

    public long getOperationalDataStoreJdbcBatchSize() {
        return getArgument(OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE, OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT);
    }

    public int orchestrationWaitIntervalSeconds() {
        return getArgument(ORCHESTRATION_WAIT_INTERVAL_SECONDS, DEFAULT_ORCHESTRATION_WAIT_INTERVAL_SECONDS);
    }

    public int orchestrationMaxAttempts() {
        return getArgument(ORCHESTRATION_MAX_ATTEMPTS, DEFAULT_ORCHESTRATION_MAX_ATTEMPTS);
    }

    public Integer getMaxObjectsPerPage() {
        int argument = getArgument(MAX_S3_PAGE_SIZE, DEFAULT_MAX_S3_PAGE_SIZE);
        if (argument > DEFAULT_MAX_S3_PAGE_SIZE || argument <= 0) {
            throw new IllegalArgumentException(MAX_S3_PAGE_SIZE + " can only be positive integer less than " + DEFAULT_MAX_S3_PAGE_SIZE);
        } else {
            return argument;
        }
    }

    public Integer getBroadcastTimeoutSeconds() {
        return getArgument(SPARK_BROADCAST_TIMEOUT_SECONDS, DEFAULT_SPARK_BROADCAST_TIMEOUT_SECONDS);
    }

    public Integer getSparkSqlMaxRecordsPerFile() {
        return getArgument(SPARK_SQL_MAX_RECORDS_PER_FILE, DEFAULT_SPARK_SQL_MAX_RECORDS_PER_FILE);
    }

    public boolean disableAutoBroadcastJoinThreshold() {
        return getArgument(DISABLE_AUTO_BROADCAST_JOIN_THRESHOLD, false);
    }

    public boolean cleanCdcCheckpoint() {
        return getArgument(CLEAN_CDC_CHECKPOINT, false);
    }

    public long getCdcTriggerIntervalSeconds() {
        return getArgument(CDC_TRIGGER_INTERVAL_SECONDS, DEFAULT_CDC_TRIGGER_INTERVAL_SECONDS);
    }

    public String getGlueTriggerName() {
        return getArgument(GLUE_TRIGGER_NAME);
    }

    public boolean activateGlueTrigger() {
        return getArgument(ACTIVATE_GLUE_TRIGGER, false);
    }

    public long streamingJobMaxFilePerTrigger() {
        return getArgument(STREAMING_JOB_MAX_FILES_PER_TRIGGER, STREAMING_JOB_DEFAULT_MAX_FILES_PER_TRIGGER);
    }

    public String getReconciliationDataSourceSourceSchemaName() {
        return getArgument(RECONCILIATION_DATASOURCE_SOURCE_SCHEMA_NAME);
    }

    public String getReconciliationDataSourceGlueConnectionName() {
        return getArgument(RECONCILIATION_DATASOURCE_GLUE_CONNECTION_NAME);
    }

    public boolean shouldReconciliationDataSourceTableNamesBeUpperCase() {
        return Boolean.parseBoolean(getArgument(RECONCILIATION_DATASOURCE_SHOULD_UPPERCASE_TABLENAMES));
    }

    public Set<ReconciliationCheck> getReconciliationChecksToRun() {
        return Optional
                .ofNullable(config.get(RECONCILIATION_CHECKS_TO_RUN))
                .map(String::toLowerCase)
                .map(s -> s.split(","))
                .map(tokens ->
                    Arrays.stream(tokens)
                            .map(String::trim)
                            .filter(s -> !s.isEmpty())
                            .map(ReconciliationCheck::fromString)
                            .collect(Collectors.toSet())
                )
                .orElse(RECONCILIATION_CHECKS_TO_RUN_DEFAULT);
    }

    public boolean shouldReconciliationFailJobIfChecksFail() {
        return getArgument(RECONCILIATION_FAIL_JOB_IF_CHECKS_FAILS, false);
    }

    public double getReconciliationChangeDataCountsToleranceRelativePercentage() {
        return getArgument(RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE, RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE_DEFAULT);
    }

    public long getReconciliationChangeDataCountsToleranceAbsolute() {
        return getArgument(RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_ABSOLUTE, RECONCILIATION_CHANGE_DATA_COUNTS_TOLERANCE_ABSOLUTE_DEFAULT);
    }

    public double getReconciliationCurrentStateCountsToleranceRelativePercentage() {
        return getArgument(RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE, RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_RELATIVE_PERCENTAGE_DEFAULT);
    }

    public long getReconciliationCurrentStateCountsToleranceAbsolute() {
        return getArgument(RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_ABSOLUTE, RECONCILIATION_CURRENT_STATE_COUNTS_TOLERANCE_ABSOLUTE_DEFAULT);
    }

    public String getCloudwatchMetricsNamespace() {
        return getArgument(CLOUDWATCH_METRICS_NAMESPACE);
    }

    public long getCloudwatchMetricsReportingPeriodSeconds() {
        return getArgument(CLOUDWATCH_METRICS_REPORTING_PERIOD_SECONDS, CLOUDWATCH_METRICS_REPORTING_PERIOD_SECONDS_DEFAULT);
    }

    public String getSecretId() {
        return config.get(SECRET_ID);
    }

    public String getTestDataTableName() {
        return getArgument(TEST_DATA_TABLE_NAME, DEFAULT_TEST_DATA_TABLE_NAME);
    }

    public int getTestDataBatchSize() {
        return getArgument(TEST_DATA_BATCH_SIZE, DEFAULT_TEST_DATA_BATCH_SIZE);
    }

    public int getTestDataParallelism() {
        return getArgument(TEST_DATA_PARALLELISM, DEFAULT_TEST_DATA_PARALLELISM);
    }

    public long getTestDataInterBatchDelayMillis() {
        return getArgument(TEST_DATA_INTER_BATCH_DELAY, DEFAULT_TEST_DATA_INTER_BATCH_DELAY_MILLIS);
    }

    public long getRunDurationMillis() {
        return getArgument(TEST_DATA_RUN_DURATION_MILLIS, DEFAULT_TEST_DATA_RUN_DURATION_MILLIS);
    }

    public boolean adjustSparkMemory() {
        return getArgument(ADJUST_SPARK_MEMORY, false);
    }

    private String getArgument(String argumentName) {
        return Optional
                .ofNullable(config.get(argumentName))
                .orElseThrow(() -> new IllegalStateException("Argument: " + argumentName + " required but not set"));
    }

    private String getArgument(String argumentName, String defaultValue) {
        return Optional
                .ofNullable(config.get(argumentName))
                .orElse(defaultValue);
    }

    private int getArgument(String argumentName, int defaultValue) {
        return Optional
                .ofNullable(config.get(argumentName))
                .map(Integer::parseInt)
                .orElse(defaultValue);
    }

    private long getArgument(String argumentName, long defaultValue) {
        return Optional
                .ofNullable(config.get(argumentName))
                .map(Long::parseLong)
                .orElse(defaultValue);
    }

    private double getArgument(String argumentName, double defaultValue) {
        return Optional
                .ofNullable(config.get(argumentName))
                .map(Double::parseDouble)
                .orElse(defaultValue);
    }

    @SuppressWarnings("unused")
    private boolean getArgument(String argumentName, boolean defaultValue) {
        return Optional
                .ofNullable(config.get(argumentName))
                .map(Boolean::parseBoolean)
                .orElse(defaultValue);
    }

    @SuppressWarnings("unused")
    private short getArgument(String argumentName, short defaultValue) {
        return Optional
                .ofNullable(config.get(argumentName))
                .map(Short::parseShort)
                .orElse(defaultValue);
    }

    // Where command line arguments are present Micronaut will create a CommandLinePropertySource instance which
    // parses any arguments and makes them available directly on the context via the getProperty() method (the args
    // are combined with properties) or via the CommandLinePropertySource instance on the environment. We prefer the
    // latter since this property source will *only* contain the arguments passed to the command.
    private static Map<String, String> getCommandLineArgumentsFromContext(ApplicationContext context) {
        return context.getEnvironment()
                .getPropertySources()
                .stream()
                .filter(JobArguments::isCommandLinePropertySource)
                .findFirst()
                .flatMap(JobArguments::castToCommandLinePropertySource)
                .map(MapPropertySource::asMap)
                .map(JobArguments::convertArgumentValuesToString)
                .orElseGet(Collections::emptyMap);
    }

    private static boolean isCommandLinePropertySource(PropertySource p) {
        return p.getName().equals(CommandLinePropertySource.NAME);
    }

    // We need to cast up to CommandLinePropertySource in order to get access to the `asMap` method.
    private static Optional<CommandLinePropertySource> castToCommandLinePropertySource(PropertySource p) {
        return (p instanceof CommandLinePropertySource commandLinePropertySource)
                ? Optional.of(commandLinePropertySource)
                : Optional.empty();
    }

    private static Map<String, String> convertArgumentValuesToString(Map<String, Object> m) {
        return m.entrySet()
                .stream()
                .map(e -> new SimpleEntry<>(e.getKey(), e.getValue().toString()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @NotNull
    private static String removeLeadingAndTrailingSlashes(String prefix) {
        if (prefix.startsWith(DELIMITER)) prefix = prefix.substring(1);
        if (prefix.endsWith(DELIMITER)) prefix = prefix.substring(0, prefix.length() - 2);
        return prefix;
    }

    private static Duration convertToPeriod(String retentionUnit, long retentionAmount, String argumentKey) {
        switch (retentionUnit.toLowerCase()) {
            case "minutes":
                return Duration.of(retentionAmount, ChronoUnit.MINUTES);
            case "hours":
                return Duration.of(retentionAmount, ChronoUnit.HOURS);
            case "days":
                return Duration.of(retentionAmount, ChronoUnit.DAYS);
            default:
                String error = String.format("Unsupported %s=%s. Allowed values are: minutes, hours, days", argumentKey, retentionUnit);
                throw new IllegalArgumentException(error);
        }
    }
}
