package uk.gov.justice.digital.client.s3;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.ConfigReaderClientException;

import java.nio.charset.StandardCharsets;

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
    private S3Client mockS3Client;
    @Mock
    private JobArguments mockArguments;
    @Captor
    private ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor;

    private S3ConfigReaderClient underTest;

    @BeforeEach
    void setup() {
        reset(mockClientProvider, mockS3Client, mockArguments);
        when(mockArguments.getConfigS3Bucket()).thenReturn(TEST_CONFIG_BUCKET);
        when(mockClientProvider.getClient()).thenReturn(mockS3Client);

        underTest = new S3ConfigReaderClient(mockClientProvider, mockArguments);
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void shouldReadConfiguredTablesWithoutDuplicates() {
        String configString = "{\"tables\": [\"schema_1/table_1\", \"schema_2/table_2\", \"schema_1/table_1\"]}";
        ResponseBytes<GetObjectResponse> response = ResponseBytes
                .fromByteArray(GetObjectResponse.builder().build(), configString.getBytes(StandardCharsets.UTF_8));
        ImmutableSet<ImmutablePair<String, String>> expectedConfiguredTables = ImmutableSet.of(
                ImmutablePair.of("schema_1", "table_1"),
                ImmutablePair.of("schema_2", "table_2")
        );

        when(mockS3Client.getObject(getObjectRequestCaptor.capture(), any(ResponseTransformer.class))).thenReturn(response);

        ImmutableSet<ImmutablePair<String, String>> result = underTest.getConfiguredTables(TEST_CONFIG_KEY);

        GetObjectRequest getObjectRequest = getObjectRequestCaptor.getValue();
        assertThat(TEST_CONFIG_BUCKET, Matchers.equalTo(getObjectRequest.bucket()));
        assertThat(CONFIGS_PATH + TEST_CONFIG_KEY + CONFIG_FILE_SUFFIX, Matchers.equalTo(getObjectRequest.key()));
        assertThat(result, containsInAnyOrder(expectedConfiguredTables.toArray()));
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void shouldThrowRuntimeExceptionWhenUnableToGetConfig() {
        when(mockS3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenThrow(SdkException.builder().message("Sdk error").build());

        assertThrows(ConfigReaderClientException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }

    @Test
    @SuppressWarnings({"unchecked"})
    void shouldThrowRuntimeExceptionWhenUnableToParseConfig() {
        String invalidConfigString = "not a json";
        ResponseBytes<GetObjectResponse> response = ResponseBytes
                .fromByteArray(GetObjectResponse.builder().build(), invalidConfigString.getBytes(StandardCharsets.UTF_8));

        when(mockS3Client.getObject(any(GetObjectRequest.class), any(ResponseTransformer.class)))
                .thenReturn(response);

        assertThrows(ConfigReaderClientException.class, () -> underTest.getConfiguredTables(TEST_CONFIG_KEY));
    }
}
