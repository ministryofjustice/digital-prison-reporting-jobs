package uk.gov.justice.digital.config;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.CommandLinePropertySource;
import io.micronaut.context.env.Environment;
import lombok.val;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO - explicit coverage recently added args
class JobArgumentsTest {

    // TODO - rename
    private static final Map<String, String> testConfig;

    static {
        testConfig = new HashMap<>();
        testConfig.put(JobArguments.AWS_KINESIS_ENDPOINT_URL, "https://kinesis.example.com");
        testConfig.put(JobArguments.AWS_REGION, "test-region");
        testConfig.put(JobArguments.CURATED_S3_PATH, "s3://somepath/curated");
        testConfig.put(JobArguments.DOMAIN_CATALOG_DATABASE_NAME, "SomeDomainCatalogName");
        testConfig.put(JobArguments.DOMAIN_NAME, "test_domain_name");
        testConfig.put(JobArguments.DOMAIN_OPERATION, "insert");
        testConfig.put(JobArguments.DOMAIN_REGISTRY, "test_registry");
        testConfig.put(JobArguments.DOMAIN_TARGET_PATH, "s3://somepath/domain/target");
        testConfig.put(JobArguments.DOMAIN_TABLE_NAME, "test_table");
        testConfig.put(JobArguments.KINESIS_READER_BATCH_DURATION_SECONDS, "5");
        testConfig.put(JobArguments.KINESIS_READER_STREAM_NAME, "some-kinesis-stream");
        testConfig.put(JobArguments.RAW_S3_PATH, "s3://somepath/raw");
        testConfig.put(JobArguments.STRUCTURED_S3_PATH, "s3://somepath/structured");
        testConfig.put(JobArguments.VIOLATIONS_S3_PATH, "s3://somepath/violations");
        testConfig.put(JobArguments.AWS_DYNAMODB_ENDPOINT_URL, "https://dynamodb.example.com");
    }

    private static final JobArguments validArguments = new JobArguments(givenAContextWithArguments(testConfig));
    private static final JobArguments emptyArguments = new JobArguments(givenAContextWithNoArguments());

    // TODO - check this - still needed?
    @Test
    public void shouldRemoveLeadingHyphensFromParameterNames() {
        val jobParameters = new JobArguments(
            Collections.singletonMap("--" + JobArguments.AWS_REGION, "test-region")
        );
        assertEquals("test-region", jobParameters.getAwsRegion());
    }

    @Test
    public void shouldReturnCorrectValueForEachSupportedArgument() {
        Map<String, Object> expectedArguments = Stream.of(new Object[][]{
                { JobArguments.AWS_DYNAMODB_ENDPOINT_URL, validArguments.getAwsDynamoDBEndpointUrl() },
                { JobArguments.AWS_KINESIS_ENDPOINT_URL, validArguments.getAwsKinesisEndpointUrl() },
                { JobArguments.AWS_REGION, validArguments.getAwsRegion() },
                { JobArguments.CURATED_S3_PATH, validArguments.getCuratedS3Path() },
                { JobArguments.DOMAIN_CATALOG_DATABASE_NAME, validArguments.getDomainCatalogDatabaseName() },
                { JobArguments.DOMAIN_NAME, validArguments.getDomainName() },
                { JobArguments.DOMAIN_OPERATION, validArguments.getDomainOperation() },
                { JobArguments.DOMAIN_REGISTRY, validArguments.getDomainRegistry() },
                { JobArguments.DOMAIN_TARGET_PATH, validArguments.getDomainTargetPath() },
                { JobArguments.DOMAIN_TABLE_NAME, validArguments.getDomainTableName() },
                { JobArguments.KINESIS_READER_STREAM_NAME, validArguments.getKinesisReaderStreamName() },
                { JobArguments.RAW_S3_PATH, validArguments.getRawS3Path() },
                { JobArguments.STRUCTURED_S3_PATH, validArguments.getStructuredS3Path() },
                { JobArguments.VIOLATIONS_S3_PATH, validArguments.getViolationsS3Path( ) },
                // Convert the Duration ms value into seconds which is what the argument uses
                { JobArguments.KINESIS_READER_BATCH_DURATION_SECONDS,
                        validArguments.getKinesisReaderBatchDuration().milliseconds() / 1000},
        }).collect(Collectors.toMap(entry -> entry[0].toString(), entry -> entry[1]));

        assertEquals(expectedArguments, testConfig);
    }

    @Test
    public void shouldThrowExceptionWhenKinesisEndpointUrlNotSet() {
        assertThrows(IllegalStateException.class, emptyArguments::getAwsKinesisEndpointUrl);
    }

    @Test
    public void shouldThrowExceptionWhenKinesisReaderStreamNameNotSet() {
        assertThrows(IllegalStateException.class, emptyArguments::getKinesisReaderStreamName);
    }


    @Test
    public void shouldThrowExceptionWhenKinesisReaderBatchDurationNotSet() {
        assertThrows(IllegalStateException.class, emptyArguments::getKinesisReaderBatchDuration);
    }

    @Test
    public void shouldThrowExceptionWhenKinesisReaderBatchDurationInvalid() {
        val underTest = new JobArguments(givenAContextWithArguments(
                Collections.singletonMap(JobArguments.KINESIS_READER_BATCH_DURATION_SECONDS, "this is not a number")
        ));

        assertThrows(NumberFormatException.class, underTest::getKinesisReaderBatchDuration);
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
<<<<<<< HEAD

    @Test
    public void shouldReturnEmptyOptionalWhenViolationsPathNotSet() {
        assertEquals(Optional.empty(), emptyArguments.getViolationsS3Path());
    }

    @Test
    public void shouldReturnOptionalWithCuratedPathWhenSet() {
        assertEquals(CURATED_S3_PATH, validArguments.getCuratedS3Path());
    }

    @Test
    public void shouldReturnOptionalWithDomainNameWhenSet() {
        assertEquals(DOMAIN_NAME, validArguments.getDomainName());
    }

    @Test
    public void shouldReturnOptionalWithDomainTableNameWhenSet() {
        assertEquals(DOMAIN_TABLE_NAME, validArguments.getDomainTableName());
    }

    @Test
    public void shouldReturnOptionalWithDomainOperationWhenSet() {
        assertEquals(DOMAIN_OPERATION, validArguments.getDomainOperation());
    }

    @Test
    public void shouldReturnOptionalWithDomainRegistryWhenSet() {
        assertEquals(DOMAIN_REGISTRY, validArguments.getDomainRegistry());
    }

    @Test
    public void shouldReturnOptionalWithDomainTargetPathWhenSet() {
        assertEquals(DOMAIN_S3_TARGET_PATH, validArguments.getDomainTargetPath());
    }

}
=======
}
>>>>>>> d8b3396 (DPR-422 parking WIP changes to review code)
