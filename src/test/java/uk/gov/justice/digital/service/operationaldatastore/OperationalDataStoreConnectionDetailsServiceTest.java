package uk.gov.justice.digital.service.operationaldatastore;

import com.amazonaws.services.glue.model.Connection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.glue.GlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreConnectionDetailsServiceTest {

    @Mock
    private GlueClient mockGlueClient;
    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private SecretsManagerClient mockSecretsManagerClient;
    @Mock
    private Connection mockConnection;

    private OperationalDataStoreConnectionDetailsService underTest;

    @BeforeEach
    public void setup() {
        underTest = new OperationalDataStoreConnectionDetailsService(mockGlueClient, mockSecretsManagerClient, mockJobArguments);
    }

    @Test
    public void shouldReturnConnectionDetails() {
        String expectedUrl = "jdbc:postgresql://localhost/test";
        String expectedUsername = "user";
        String expectedPassword = "pass";
        String connectionName = "some-connection-name";
        String secretId = "some-secret-id";

        Map<String, String> connectionProperties = new HashMap<>();
        connectionProperties.put("JDBC_CONNECTION_URL", expectedUrl);
        connectionProperties.put("SECRET_ID", secretId);

        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername(expectedUsername);
        credentials.setPassword(expectedPassword);

        when(mockJobArguments.getOperationalDataStoreGlueConnectionName()).thenReturn(connectionName);
        when(mockGlueClient.getConnection(connectionName)).thenReturn(mockConnection);
        when(mockConnection.getConnectionProperties()).thenReturn(connectionProperties);
        when(mockSecretsManagerClient.getSecret(secretId, OperationalDataStoreCredentials.class)).thenReturn(credentials);

        OperationalDataStoreConnectionDetails result = underTest.getConnectionDetails();
        assertEquals(expectedUrl, result.getUrl());
        assertEquals(expectedUsername, result.getCredentials().getUsername());
        assertEquals(expectedPassword, result.getCredentials().getPassword());

        verify(mockGlueClient, times(1)).getConnection(connectionName);
        verify(mockSecretsManagerClient, times(1)).getSecret(secretId, OperationalDataStoreCredentials.class);
    }

}