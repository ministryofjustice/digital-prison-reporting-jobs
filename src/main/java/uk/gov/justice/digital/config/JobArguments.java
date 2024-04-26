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

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.client.s3.S3FileTransferClient.DELIMITER;

/**
 * Class that defines and provides access to the job arguments that we support.
 * Arguments are taken from the values parsed by the CommandLinePropertySource.
 */
@Singleton
public class JobArguments {

    private static final Logger logger = LoggerFactory.getLogger(JobArguments.class);

    public static final String CONFIG_S3_BUCKET = "dpr.config.s3.bucket";
    public static final String CONFIG_KEY = "dpr.config.key";
    public static final String AWS_DYNAMODB_ENDPOINT_URL = "dpr.aws.dynamodb.endpointUrl";
    public static final String AWS_REGION = "dpr.aws.region";
    public static final String LOG_LEVEL = "dpr.log.level";
    public static final String CONTRACT_REGISTRY_NAME = "dpr.contract.registryName";
    public static final String SCHEMA_CACHE_MAX_SIZE = "dpr.schema.cache.max.size";
    public static final String SCHEMA_CACHE_EXPIRY_IN_MINUTES = "dpr.schema.cache.expiry.days";
    public static final String CURATED_S3_PATH = "dpr.curated.s3.path";
    public static final String PRISONS_DATA_SWITCH_TARGET_S3_PATH = "dpr.prisons.data.switch.target.s3.path";
    // Raw files that have been processed by the streaming job are moved to this location before being archived.
    // Spark structured streaming only allows moving processed files to a location outside the source to avoid recycling the files.
    // It should be noted that the processed files can only be moved to a location within the same s3 bucket.
    public static final String PROCESSED_RAW_FILES_PATH = "dpr.processed.raw.files.path";
    public static final String DOMAIN_CATALOG_DATABASE_NAME = "dpr.domain.catalog.db";
    public static final String DOMAIN_NAME = "dpr.domain.name";
    public static final String DOMAIN_OPERATION = "dpr.domain.operation";
    public static final String DOMAIN_REGISTRY = "dpr.domain.registry";
    public static final String DOMAIN_TARGET_PATH = "dpr.domain.target.path";
    public static final String DOMAIN_TABLE_NAME = "dpr.domain.table.name";
    public static final String BATCH_DURATION_SECONDS = "dpr.batchDurationSeconds";
    public static final String KINESIS_STREAM_ARN = "dpr.kinesis.stream.arn";
    public static final String KINESIS_STARTING_POSITION = "dpr.kinesis.starting.position";
    // See https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-kinesis-home.html
    // The possible values are "latest", "trim_horizon", "earliest", or a Timestamp string in UTC format
    // in the pattern yyyy-mm-ddTHH:MM:SSZ
    // (where Z represents a UTC timezone offset with a +/-. For example "2023-04-04T08:00:00-04:00").
    // We default to trim_horizon to avoid data loss
    public static final String KINESIS_STARTING_POSITION_DEFAULT = "trim_horizon";
    // See https://docs.aws.amazon.com/glue/latest/webapi/API_KinesisStreamingSourceOptions.html
    // dpr.add.idle.time.between.reads defaults to false thereby causing IdleTimeBetweenReadsInMs to default to 0 ms
    public static final String ADD_IDLE_TIME_BETWEEN_READS = "dpr.add.idle.time.between.reads";
    // The provided value for dpr.idle.time.between.reads.millis is only applied when dpr.add.idle.time.between.reads is true
    public static final String IDLE_TIME_BETWEEN_READS_IN_MILLIS = "dpr.idle.time.between.reads.millis";
    public static final String RAW_S3_PATH = "dpr.raw.s3.path";
    public static final String RAW_ARCHIVE_S3_PATH = "dpr.raw.archive.s3.path";
    public static final String STRUCTURED_S3_PATH = "dpr.structured.s3.path";
    public static final String VIOLATIONS_S3_PATH = "dpr.violations.s3.path";
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
    public static final String FILE_TRANSFER_RETENTION_DAYS = "dpr.file.transfer.retention.days";
    static final Long DEFAULT_FILE_TRANSFER_RETENTION_DAYS = 0L;

    public static final String FILE_TRANSFER_DELETE_COPIED_FILES_FLAG = "dpr.file.transfer.delete.copied.files";

    // A comma separated list of buckets to delete files from
    static final String FILE_DELETION_BUCKETS = "dpr.file.deletion.buckets";
    // A comma separated list of s3 file extensions. The wildcard '*' includes all extensions.
    static final String ALLOWED_S3_FILE_EXTENSIONS = "dpr.allowed.s3.file.extensions";
    static final String GLUE_ORCHESTRATION_WAIT_INTERVAL_SECONDS = "dpr.glue.orchestration.wait.interval.seconds";
    static final int DEFAULT_GLUE_ORCHESTRATION_WAIT_INTERVAL_SECONDS = 10;
    static final String GLUE_ORCHESTRATION_MAX_ATTEMPTS = "dpr.glue.orchestration.max.attempts";
    static final int DEFAULT_GLUE_ORCHESTRATION_MAX_ATTEMPTS = 20;
    static final String STOP_GLUE_INSTANCE_JOB_NAME = "dpr.stop.glue.instance.job.name";
    static final String MAX_S3_PAGE_SIZE = "dpr.s3.max.page.size";
    static final Integer DEFAULT_MAX_S3_PAGE_SIZE = 1000;
    static final String CLEAN_CDC_CHECKPOINT = "dpr.clean.cdc.checkpoint";
    static final String SPARK_BROADCAST_TIMEOUT_SECONDS = "dpr.spark.broadcast.timeout.seconds";
    static final Integer DEFAULT_SPARK_BROADCAST_TIMEOUT_SECONDS = 300;

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

    public String getKinesisStartingPosition() {
        return getArgument(KINESIS_STARTING_POSITION, KINESIS_STARTING_POSITION_DEFAULT);
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

    public String getKinesisStreamArn() {
        return getArgument(KINESIS_STREAM_ARN);
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

    public String getProcessedRawFilesPath() {
        return getArgument(PROCESSED_RAW_FILES_PATH);
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

    public String getConfigKey() {
        return getArgument(CONFIG_KEY);
    }

    public Optional<String> getOptionalConfigKey() {
        return Optional.ofNullable(config.get(CONFIG_KEY));
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

    public Long getFileTransferRetentionDays() {
        return getArgument(FILE_TRANSFER_RETENTION_DAYS, DEFAULT_FILE_TRANSFER_RETENTION_DAYS);
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

    public ImmutableSet<String> getAllowedS3FileExtensions() {
        Set<String> extensions = Arrays.stream(getArgument(ALLOWED_S3_FILE_EXTENSIONS).toLowerCase().split(","))
                .map(item -> item.trim().toLowerCase())
                .filter(item -> !item.isEmpty())
                .collect(Collectors.toSet());

        if (extensions.isEmpty()) throw new IllegalStateException("Argument " + ALLOWED_S3_FILE_EXTENSIONS + " evaluated to empty set");

        if (extensions.contains("*")) {
            return ImmutableSet.of("*");
        } else {
            return ImmutableSet.copyOf(extensions);
        }
    }

    public String getStopGlueInstanceJobName() {
        return getArgument(STOP_GLUE_INSTANCE_JOB_NAME);
    }

    public int glueOrchestrationWaitIntervalSeconds() {
        return getArgument(GLUE_ORCHESTRATION_WAIT_INTERVAL_SECONDS, DEFAULT_GLUE_ORCHESTRATION_WAIT_INTERVAL_SECONDS);
    }

    public int glueOrchestrationMaxAttempts() {
        return getArgument(GLUE_ORCHESTRATION_MAX_ATTEMPTS, DEFAULT_GLUE_ORCHESTRATION_MAX_ATTEMPTS);
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

    public boolean cleanCdcCheckpoint() {
        return getArgument(CLEAN_CDC_CHECKPOINT, false);
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
        return (p instanceof CommandLinePropertySource)
                ? Optional.of((CommandLinePropertySource) p)
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
}