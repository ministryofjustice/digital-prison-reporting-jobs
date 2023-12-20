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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.client.s3.S3ConfigReaderClient.CONFIG_FILE_NAME;

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
    public void setup() {
        reset(mockClientProvider, mockS3Client, mockArguments);
        when(mockArguments.getConfigS3Bucket()).thenReturn(TEST_CONFIG_BUCKET);
        when(mockClientProvider.getClient()).thenReturn(mockS3Client);

        underTest = new S3ConfigReaderClient(mockClientProvider, mockArguments);
    }

    @Test
    public void shouldReadConfiguredTablesWithoutDuplicates() {
        String configString = "{\"" + TEST_CONFIG_KEY + "\": [\"schema_1/table_1\", \"schema_2/table_2\", \"schema_1/table_1\"]}";
        ImmutableSet<ImmutablePair<String, String>> expectedConfiguredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockS3Client.getObjectAsString(eq(TEST_CONFIG_BUCKET), eq(CONFIG_FILE_NAME))).thenReturn(configString);

        ImmutableSet<ImmutablePair<String, String>> result = underTest.getConfiguredTables(TEST_CONFIG_KEY);

        assertThat(result, containsInAnyOrder(expectedConfiguredTables.toArray()));
    }

    @Test
    public void shouldThrowRuntimeExceptionWhenUnableToGetConfig() {
        when(mockS3Client.getObjectAsString(any(), any())).thenThrow(new SdkBaseException("Sdk error"));

        assertThrows(RuntimeException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }

    @Test
    public void shouldThrowRuntimeExceptionWhenUnableToParseConfig() {
        String invalidConfigString = "not a json";

        when(mockS3Client.getObjectAsString(any(), any())).thenReturn(invalidConfigString);

        assertThrows(RuntimeException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }
}