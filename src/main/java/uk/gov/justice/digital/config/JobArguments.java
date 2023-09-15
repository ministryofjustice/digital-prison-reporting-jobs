package uk.gov.justice.digital.config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.CommandLinePropertySource;
import io.micronaut.context.env.MapPropertySource;
import io.micronaut.context.env.PropertySource;
import io.micronaut.logging.LogLevel;
import lombok.val;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class that defines and provides access to the job arguments that we support.
 *
 * Arguments are taken from the values parsed by the CommandLinePropertySource.
 */
@Singleton
public class JobArguments {

    private static final Logger logger = LoggerFactory.getLogger(JobArguments.class);

    public static final String AWS_DYNAMODB_ENDPOINT_URL = "dpr.aws.dynamodb.endpointUrl";
    public static final String AWS_KINESIS_ENDPOINT_URL = "dpr.aws.kinesis.endpointUrl";
    public static final String AWS_REGION = "dpr.aws.region";
    public static final String LOG_LEVEL = "dpr.log.level";
    public static final String CONTRACT_REGISTRY_NAME = "dpr.contract.registryName";
    public static final String CURATED_S3_PATH = "dpr.curated.s3.path";
    public static final String DOMAIN_CATALOG_DATABASE_NAME = "dpr.domain.catalog.db";
    public static final String DOMAIN_NAME = "dpr.domain.name";
    public static final String DOMAIN_OPERATION = "dpr.domain.operation";
    public static final String DOMAIN_REGISTRY = "dpr.domain.registry";
    public static final String DOMAIN_TARGET_PATH = "dpr.domain.target.path";
    public static final String DOMAIN_TABLE_NAME = "dpr.domain.table.name";
    public static final String KINESIS_READER_BATCH_DURATION_SECONDS = "dpr.kinesis.reader.batchDurationSeconds";
    public static final String KINESIS_READER_STREAM_NAME = "dpr.kinesis.reader.streamName";
    public static final String RAW_S3_PATH = "dpr.raw.s3.path";
    public static final String STRUCTURED_S3_PATH = "dpr.structured.s3.path";
    public static final String VIOLATIONS_S3_PATH = "dpr.violations.s3.path";
    public static final String REDSHIFT_SECRETS_NAME = "dpr.redshift.secrets.name";
    public static final String DATA_MART_DB_NAME = "dpr.datamart.db.name";
    public static final String MAINTENANCE_TABLES_ROOT_PATH = "dpr.maintenance.root.path";

    private final Map<String, String> config;

    public Map<String, String> getConfig() {
        // FIXME: reference escapes!
        return this.config;
    }

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

    public String getAwsRegion() {
        return getArgument(AWS_REGION);
    }

    public String getAwsKinesisEndpointUrl() {
        return getArgument(AWS_KINESIS_ENDPOINT_URL);
    }

    public String getAwsDynamoDBEndpointUrl() {
        return getArgument(AWS_DYNAMODB_ENDPOINT_URL);
    }

    public String getKinesisReaderStreamName() {
        return getArgument(KINESIS_READER_STREAM_NAME);
    }

    public Duration getKinesisReaderBatchDuration() {
        val durationSeconds = getArgument(KINESIS_READER_BATCH_DURATION_SECONDS);
        val parsedDuration = Long.parseLong(durationSeconds);
        return Durations.seconds(parsedDuration);
    }

    public String getRawS3Path() {
        return getArgument(RAW_S3_PATH);
    }

    public String getStructuredS3Path() {
        return getArgument(STRUCTURED_S3_PATH);
    }

    public String getViolationsS3Path() {
        return getArgument(VIOLATIONS_S3_PATH);
    }

    public String getCuratedS3Path() {
        return getArgument(CURATED_S3_PATH);
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

    public String getRedshiftSecretsName() { return getArgument(REDSHIFT_SECRETS_NAME); }

    public String getDataMartDbName() { return getArgument(DATA_MART_DB_NAME); }

    public String getMaintenanceTablesRootPath() {
        return getArgument(MAINTENANCE_TABLES_ROOT_PATH);
    }

    private String getArgument(String argumentName) {
        return Optional
                .ofNullable(config.get(argumentName))
                .orElseThrow(() -> new IllegalStateException("Argument: " + argumentName + " required but not set"));
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

}