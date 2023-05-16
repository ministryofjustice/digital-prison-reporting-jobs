package uk.gov.justice.digital.config;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.CommandLinePropertySource;
import io.micronaut.context.env.Environment;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// TODO - explicit coverage recently added args
class JobArgumentsTest {

    private static final Map<String, String> testArguments = Stream.of(new String[][] {
            { JobArguments.AWS_KINESIS_ENDPOINT_URL, "https://kinesis.example.com" },
            { JobArguments.AWS_REGION, "test-region" },
            { JobArguments.CURATED_S3_PATH, "s3://somepath/curated" },
            { JobArguments.DOMAIN_CATALOG_DATABASE_NAME, "SomeDomainCatalogName" },
            { JobArguments.DOMAIN_NAME, "test_domain_name" },
            { JobArguments.DOMAIN_OPERATION, "insert" },
            { JobArguments.DOMAIN_REGISTRY, "test_registry" },
            { JobArguments.DOMAIN_TARGET_PATH, "s3://somepath/domain/target" },
            { JobArguments.DOMAIN_TABLE_NAME, "test_table" },
            { JobArguments.KINESIS_READER_BATCH_DURATION_SECONDS, "5" },
            { JobArguments.KINESIS_READER_STREAM_NAME, "some-kinesis-stream" },
            { JobArguments.RAW_S3_PATH, "s3://somepath/raw" },
            { JobArguments.STRUCTURED_S3_PATH, "s3://somepath/structured" },
            { JobArguments.VIOLATIONS_S3_PATH, "s3://somepath/violations" },
            { JobArguments.AWS_DYNAMODB_ENDPOINT_URL, "https://dynamodb.example.com" }
    }).collect(Collectors.toMap(e -> e[0], e -> e[1]));

    private static final JobArguments validArguments = new JobArguments(givenAContextWithArguments(testArguments));
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
        Map<String, String> actualArguments = Stream.of(new Object[][] {
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
                // Convert the Duration ms value into seconds to align with the argument.
                { JobArguments.KINESIS_READER_BATCH_DURATION_SECONDS,
                        validArguments.getKinesisReaderBatchDuration().milliseconds() / 1000},
        }).collect(Collectors.toMap(entry -> entry[0].toString(), entry -> entry[1].toString()));

        assertEquals(testArguments, actualArguments);
    }

    @Test
    public void showThrowAnExceptionWhenAMissingArgumentIsRequested() {
        assertThrows(IllegalStateException.class, emptyArguments::getAwsRegion);
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
}