package uk.gov.justice.digital.config;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.CommandLinePropertySource;
import io.micronaut.context.env.MapPropertySource;
import io.micronaut.context.env.PropertySource;
import lombok.val;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
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
    public static final String CURATED_S3_PATH = "dpr.curated.s3.path";
    public static final String DOMAIN_CATALOG_DATABASE_NAME = "dpr.domain.catalog.db";
    public static final String DOMAIN_NAME = "dpr.domain.name";
    public static final String DOMAIN_OPERATION = "dpr.domain.operation";
    public static final String DOMAIN_REGISTRY = "dpr.domain.registry";
    public static final String DOMAIN_S3_TARGET_PATH = "dpr.domain.target.path";
    public static final String DOMAIN_TABLE_NAME = "dpr.domain.table.name";
    public static final String KINESIS_READER_BATCH_DURATION_SECONDS = "dpr.kinesis.reader.batchDurationSeconds";
    public static final String KINESIS_READER_STREAM_NAME = "dpr.kinesis.reader.streamName";
    public static final String RAW_S3_PATH = "dpr.raw.s3.path";
    public static final String STRUCTURED_S3_PATH = "dpr.structured.s3.path";
    public static final String VIOLATIONS_S3_PATH = "dpr.violations.s3.path";

    private final Map<String, String> config;

    @Inject
    public JobArguments(ApplicationContext context) {
        this(getCommandLineArgumentsFromContext(context));
    }

    public JobArguments(Map<String, String> config) {
        this.config = config.entrySet()
                .stream()
                .map(this::cleanEntryKey)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        logger.info("Job initialised with parameters: {}", config);
    }

    public String getAwsRegion() {
        return getMandatoryProperty(AWS_REGION);
    }

    public String getAwsKinesisEndpointUrl() {
        return getMandatoryProperty(AWS_KINESIS_ENDPOINT_URL);
    }

    public String getAwsDynamoDBEndpointUrl() {
        return getMandatoryProperty(AWS_DYNAMODB_ENDPOINT_URL);
    }

    public String getKinesisReaderStreamName() {
        return getMandatoryProperty(KINESIS_READER_STREAM_NAME);
    }

    public Duration getKinesisReaderBatchDuration() {
        String durationSeconds = getMandatoryProperty(KINESIS_READER_BATCH_DURATION_SECONDS);
        long parsedDuration = Long.parseLong(durationSeconds);
        return Durations.seconds(parsedDuration);
    }

    public Optional<String> getRawS3Path() {
        return getOptionalProperty(RAW_S3_PATH);
    }

    public Optional<String> getStructuredS3Path() {
        return getOptionalProperty(STRUCTURED_S3_PATH);
    }

    public Optional<String> getViolationsS3Path() {
        return getOptionalProperty(VIOLATIONS_S3_PATH);
    }

    public String getCuratedS3Path() {
        return getMandatoryProperty(CURATED_S3_PATH);
    }

    public String getDomainTargetPath() {
        return getMandatoryProperty(DOMAIN_S3_TARGET_PATH);
    }

    public String getDomainName() {
        return getMandatoryProperty(DOMAIN_NAME);
    }

    public String getDomainTableName() {
        return getMandatoryProperty(DOMAIN_TABLE_NAME);
    }

    public String getDomainRegistry() {
        return getMandatoryProperty(DOMAIN_REGISTRY);
    }

    public String getDomainOperation() {
        return getMandatoryProperty(DOMAIN_OPERATION);
    }

    public Optional<String> getDomainCatalogDatabaseName() {
        return getOptionalProperty(DOMAIN_CATALOG_DATABASE_NAME);
    }

    private String getMandatoryProperty(String jobParameter) {
        return Optional
                .ofNullable(config.get(jobParameter))
                .orElseThrow(() -> new IllegalStateException("Job Parameter: " + jobParameter + " is not set"));
    }

    // TODO - consider supporting a default value where if no value is provided we throw an exception if there is no
    //        value at all
    private Optional<String> getOptionalProperty(String jobParameter) {
        return Optional
                .ofNullable(config.get(jobParameter));
    }

    // We expect job parameters to be specified with a leading -- prefix e.g. --some.job.setting consistent with how
    // AWS glue specifies job parameters. The prefix is removed to clean up code handling parameters by name.
    private Map.Entry<String, String> cleanEntryKey(Map.Entry<String, String> entry) {
        // TODO - check this - we may not need this
        val cleanedKey = entry.getKey().replaceFirst("--", "");
        return new SimpleEntry<>(cleanedKey, entry.getValue());
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
