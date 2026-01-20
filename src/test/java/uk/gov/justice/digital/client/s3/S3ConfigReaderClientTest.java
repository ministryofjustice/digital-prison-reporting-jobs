package uk.gov.justice.digital.client.s3;

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.AmazonS3;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.ConfigReaderClientException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.client.s3.S3ConfigReaderClient.CONFIGS_PATH;
import static uk.gov.justice.digital.client.s3.S3ConfigReaderClient.CONFIG_FILE_SUFFIX;

@ExtendWith(MockitoExtension.class)
class S3ConfigReaderClientTest {

    private static final String TEST_CONFIG_KEY = "some-config-key";
    private static final String TEST_CONFIG_BUCKET = "some-config-bucket";

    @Mock
    private S3ClientProvider mockClientProvider;
    @Mock
    private AmazonS3 mockS3Client;
    @Mock
    private JobArguments mockArguments;

    private S3ConfigReaderClient underTest;

    @BeforeEach
    void setup() {
        reset(mockClientProvider, mockS3Client, mockArguments);
        when(mockArguments.getConfigS3Bucket()).thenReturn(TEST_CONFIG_BUCKET);
        when(mockClientProvider.getClient()).thenReturn(mockS3Client);

        underTest = new S3ConfigReaderClient(mockClientProvider, mockArguments);
    }

    @Test
    void shouldReadConfiguredTablesWithoutDuplicates() {
        String configString = "{\"tables\": [\"schema_1/table_1\", \"schema_2/table_2\", \"schema_1/table_1\"]}";
        ImmutableSet<ImmutablePair<String, String>> expectedConfiguredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockS3Client.getObjectAsString(TEST_CONFIG_BUCKET, CONFIGS_PATH + TEST_CONFIG_KEY + CONFIG_FILE_SUFFIX))
                .thenReturn(configString);

        ImmutableSet<ImmutablePair<String, String>> result = underTest.getConfiguredTables(TEST_CONFIG_KEY);

        assertThat(result, containsInAnyOrder(expectedConfiguredTables.toArray()));
    }

    @Test
    void shouldThrowRuntimeExceptionWhenUnableToGetConfig() {
        when(mockS3Client.getObjectAsString(any(), any())).thenThrow(new SdkBaseException("Sdk error"));

        assertThrows(ConfigReaderClientException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }

    @Test
    void shouldThrowRuntimeExceptionWhenUnableToParseConfig() {
        String invalidConfigString = "not a json";

        when(mockS3Client.getObjectAsString(any(), any())).thenReturn(invalidConfigString);

        assertThrows(ConfigReaderClientException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }
}
