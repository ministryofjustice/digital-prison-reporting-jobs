package uk.gov.justice.digital.client.glue;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetSchemaVersionResult;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GlueSchemaClientTest {

    private static final String SCHEMA_NAME = "somesource.sometable";
    private static final String FAKE_SCHEMA_DEFINITION = "This is a fake schema definition";

    @Mock
    private GlueClientProvider mockClientProvider;

    @Mock
    private AWSGlue mockClient;

    @Mock
    private JobArguments mockArguments;

    @Mock
    private GetSchemaVersionResult mockResponse;

    private GlueSchemaClient underTest;

    @BeforeEach
    public void setup() {
        givenJobArgumentsReturnAContractRegistryName();
        givenClientProviderReturnsAClient();
        underTest = new GlueSchemaClient(mockClientProvider, mockArguments);
    }

    @Test
    public void shouldReturnASchemaForAValidRequest() {
        givenClientReturnsAValidResponse();

        val result = underTest.getSchema(SCHEMA_NAME);

        assertEquals(Optional.of(FAKE_SCHEMA_DEFINITION), result);
    }

    @Test
    public void shouldReturnAnEmptyOptionalForASchemaThatDoesNotExist() {
        givenClientThrowsAnEntityNotFoundException();

        val result = underTest.getSchema(SCHEMA_NAME);

        assertFalse(result.isPresent());
    }

    @Test
    public void shouldThrowAnExceptionIfAnyOtherErrorOccurs() {
        givenClientThrowsSomeOtherException();

        assertThrows(SdkClientException.class, () -> underTest.getSchema(SCHEMA_NAME));
    }

    private void givenClientProviderReturnsAClient() {
        when(mockClientProvider.getClient()).thenReturn(mockClient);
    }

    private void givenJobArgumentsReturnAContractRegistryName() {
        when(mockArguments.getContractRegistryName()).thenReturn("test-contract-registry");
    }

    private void givenClientReturnsAValidResponse() {
        when(mockResponse.getSchemaDefinition()).thenReturn(FAKE_SCHEMA_DEFINITION);
        when(mockClient.getSchemaVersion(any())).thenReturn(mockResponse);
    }

    private void givenClientThrowsAnEntityNotFoundException() {
        when(mockClient.getSchemaVersion(any())).thenThrow(new EntityNotFoundException("Schema not found"));
    }

    private void givenClientThrowsSomeOtherException() {
        when(mockClient.getSchemaVersion(any())).thenThrow(new SdkClientException("Some other client error"));
    }

}