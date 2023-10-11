package uk.gov.justice.digital.config;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.CommandLinePropertySource;
import io.micronaut.context.env.Environment;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JobArgumentsIntegrationTest {

    private static final Map<String, String> testArguments = Stream.of(new String[][] {
            { JobArguments.AWS_REGION, "test-region" },
            { JobArguments.CURATED_S3_PATH, "s3://somepath/curated" },
            { JobArguments.DOMAIN_CATALOG_DATABASE_NAME, "SomeDomainCatalogName" },
            { JobArguments.DOMAIN_NAME, "test_domain_name" },
            { JobArguments.DOMAIN_OPERATION, "insert" },
            { JobArguments.DOMAIN_REGISTRY, "test_registry" },
            { JobArguments.DOMAIN_TARGET_PATH, "s3://somepath/domain/target" },
            { JobArguments.DOMAIN_TABLE_NAME, "test_table" },
            { JobArguments.RAW_S3_PATH, "s3://somepath/raw" },
            { JobArguments.STRUCTURED_S3_PATH, "s3://somepath/structured" },
            { JobArguments.VIOLATIONS_S3_PATH, "s3://somepath/violations" },
            { JobArguments.AWS_DYNAMODB_ENDPOINT_URL, "https://dynamodb.example.com" },
            { JobArguments.CONTRACT_REGISTRY_NAME, "SomeContractRegistryName" },
            { JobArguments.CHECKPOINT_LOCATION, "s3://somepath/checkpoint/app-name" },
            { JobArguments.KINESIS_STREAM_ARN, "arn:aws:kinesis:eu-west-2:123456:stream/dpr-kinesis-ingestor-env" },
            { JobArguments.KINESIS_STARTING_POSITION, "trim_horizon" },
            { JobArguments.ADD_IDLE_TIME_BETWEEN_READS, "true" },
            { JobArguments.BATCH_MAX_RETRIES, "5" },
            { JobArguments.LOG_LEVEL, "debug" },
            { JobArguments.MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH, "1" },
    }).collect(Collectors.toMap(e -> e[0], e -> e[1]));

    private static final JobArguments validArguments = new JobArguments(givenAContextWithArguments(testArguments));
    private static final JobArguments emptyArguments = new JobArguments(givenAContextWithNoArguments());

    @Test
    public void shouldReturnCorrectValueForEachSupportedArgument() {
        Map<String, String> actualArguments = Stream.of(new Object[][] {
                { JobArguments.AWS_DYNAMODB_ENDPOINT_URL, validArguments.getAwsDynamoDBEndpointUrl() },
                { JobArguments.AWS_REGION, validArguments.getAwsRegion() },
                { JobArguments.CURATED_S3_PATH, validArguments.getCuratedS3Path() },
                { JobArguments.DOMAIN_CATALOG_DATABASE_NAME, validArguments.getDomainCatalogDatabaseName() },
                { JobArguments.DOMAIN_NAME, validArguments.getDomainName() },
                { JobArguments.DOMAIN_OPERATION, validArguments.getDomainOperation() },
                { JobArguments.DOMAIN_REGISTRY, validArguments.getDomainRegistry() },
                { JobArguments.DOMAIN_TARGET_PATH, validArguments.getDomainTargetPath() },
                { JobArguments.DOMAIN_TABLE_NAME, validArguments.getDomainTableName() },
                { JobArguments.RAW_S3_PATH, validArguments.getRawS3Path() },
                { JobArguments.STRUCTURED_S3_PATH, validArguments.getStructuredS3Path() },
                { JobArguments.VIOLATIONS_S3_PATH, validArguments.getViolationsS3Path() },
                { JobArguments.CONTRACT_REGISTRY_NAME, validArguments.getContractRegistryName() },
                { JobArguments.CHECKPOINT_LOCATION, validArguments.getCheckpointLocation() },
                { JobArguments.KINESIS_STREAM_ARN, validArguments.getKinesisStreamArn() },
                { JobArguments.KINESIS_STARTING_POSITION, validArguments.getKinesisStartingPosition() },
                { JobArguments.ADD_IDLE_TIME_BETWEEN_READS, validArguments.addIdleTimeBetweenReads() },
                { JobArguments.BATCH_MAX_RETRIES, Integer.toString(validArguments.getBatchMaxRetries()) },
                { JobArguments.LOG_LEVEL, validArguments.getLogLevel().toString().toLowerCase() },
                { JobArguments.MAINTENANCE_LIST_TABLE_RECURSE_MAX_DEPTH, Integer.toString(validArguments.getMaintenanceListTableRecurseMaxDepth()) },
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

    @Test
    public void shouldSetAllowedLogLevels() {
        for(String level: Arrays.asList("debug", "info", "warn", "error", "DEBUG", "INFO", "WARN", "ERROR")) {
            HashMap<String, String> args = cloneTestArguments();
            args.put(JobArguments.LOG_LEVEL, level);
            JobArguments jobArguments = new JobArguments(givenAContextWithArguments(args));
            assertEquals(level.toUpperCase(), jobArguments.getLogLevel().toString().toUpperCase());
        }
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

    private static HashMap<String, String> cloneTestArguments() {
        return (HashMap<String, String>)((HashMap<String, String>) testArguments).clone();
    }
}