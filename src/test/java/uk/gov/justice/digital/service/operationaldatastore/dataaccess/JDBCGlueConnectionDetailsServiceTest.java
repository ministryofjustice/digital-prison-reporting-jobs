package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.glue.model.Connection;
import software.amazon.awssdk.services.glue.model.ConnectionPropertyKey;
import uk.gov.justice.digital.client.glue.DefaultGlueClient;
import uk.gov.justice.digital.client.secretsmanager.SecretsClient;
import uk.gov.justice.digital.datahub.model.JDBCCredentials;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;
import uk.gov.justice.digital.exception.JDBCGlueConnectionDetailsException;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;

import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class JDBCGlueConnectionDetailsServiceTest {

    private static final String CONNECTION_NAME = "some-connection-name";
    @Mock
    private DefaultGlueClient mockGlueClient;
    @Mock
    private SecretsClient mockSecretsManagerClient;
    @Mock
    private Connection mockConnection;

    private JDBCGlueConnectionDetailsService underTest;

    @BeforeEach
    void setUp() {
        underTest = new JDBCGlueConnectionDetailsService(mockGlueClient, mockSecretsManagerClient);
    }

    @Test
    void shouldReturnConnectionDetails() {
        String expectedUrl = "jdbc:postgresql://localhost/test";
        String expectedDriver = "org.postgresql.Driver";
        String expectedUsername = "user";
        String expectedPassword = "pass";
        String secretId = "some-secret-id";

        Map<ConnectionPropertyKey, String> connectionProperties = new EnumMap<>(ConnectionPropertyKey.class);
        connectionProperties.put(ConnectionPropertyKey.JDBC_CONNECTION_URL, expectedUrl);
        connectionProperties.put(ConnectionPropertyKey.JDBC_DRIVER_CLASS_NAME, expectedDriver);
        connectionProperties.put(ConnectionPropertyKey.SECRET_ID, secretId);

        JDBCCredentials credentials = new JDBCCredentials(expectedUsername, expectedPassword);

        when(mockGlueClient.getConnection(CONNECTION_NAME)).thenReturn(mockConnection);
        when(mockConnection.connectionProperties()).thenReturn(connectionProperties);
        when(mockSecretsManagerClient.getSecret(secretId, JDBCCredentials.class)).thenReturn(credentials);

        JDBCGlueConnectionDetails result = underTest.getConnectionDetails(CONNECTION_NAME);
        assertEquals(expectedUrl, result.getUrl());
        assertEquals(expectedDriver, result.getJdbcDriverClassName());
        assertEquals(expectedUsername, result.getCredentials().getUsername());
        assertEquals(expectedPassword, result.getCredentials().getPassword());

        verify(mockGlueClient, times(1)).getConnection(CONNECTION_NAME);
        verify(mockSecretsManagerClient, times(1)).getSecret(secretId, JDBCCredentials.class);
    }

    @Test
    void shouldThrowForNullJdbcConnectionUrl() {

        Map<ConnectionPropertyKey, String> connectionProperties = new EnumMap<>(ConnectionPropertyKey.class);
        connectionProperties.put(ConnectionPropertyKey.JDBC_CONNECTION_URL, null);
        connectionProperties.put(ConnectionPropertyKey.JDBC_DRIVER_CLASS_NAME, "driver");
        connectionProperties.put(ConnectionPropertyKey.SECRET_ID, "secret");

        when(mockGlueClient.getConnection(CONNECTION_NAME)).thenReturn(mockConnection);
        when(mockConnection.connectionProperties()).thenReturn(connectionProperties);

        assertThrows(JDBCGlueConnectionDetailsException.class, () -> underTest.getConnectionDetails(CONNECTION_NAME));
    }

    @Test
    void shouldThrowForNullDriverClassName() {

        Map<ConnectionPropertyKey, String> connectionProperties = new EnumMap<>(ConnectionPropertyKey.class);
        connectionProperties.put(ConnectionPropertyKey.JDBC_CONNECTION_URL, "connection url");
        connectionProperties.put(ConnectionPropertyKey.JDBC_DRIVER_CLASS_NAME, null);
        connectionProperties.put(ConnectionPropertyKey.SECRET_ID, "secret");

        when(mockGlueClient.getConnection(CONNECTION_NAME)).thenReturn(mockConnection);
        when(mockConnection.connectionProperties()).thenReturn(connectionProperties);

        assertThrows(JDBCGlueConnectionDetailsException.class, () -> underTest.getConnectionDetails(CONNECTION_NAME));
    }

    @Test
    void shouldThrowForNullUsername() {
        String secretId = "some-secret-id";

        Map<ConnectionPropertyKey, String> connectionProperties = new EnumMap<>(ConnectionPropertyKey.class);
        connectionProperties.put(ConnectionPropertyKey.JDBC_CONNECTION_URL, "connectionurl");
        connectionProperties.put(ConnectionPropertyKey.JDBC_DRIVER_CLASS_NAME, "driver");
        connectionProperties.put(ConnectionPropertyKey.SECRET_ID, secretId);

        JDBCCredentials credentials = new JDBCCredentials(null, "password");

        when(mockGlueClient.getConnection(CONNECTION_NAME)).thenReturn(mockConnection);
        when(mockConnection.connectionProperties()).thenReturn(connectionProperties);
        when(mockSecretsManagerClient.getSecret(secretId, JDBCCredentials.class)).thenReturn(credentials);

        assertThrows(JDBCGlueConnectionDetailsException.class, () -> underTest.getConnectionDetails(CONNECTION_NAME));
    }

    @Test
    void shouldThrowForNullPassword() {
        String secretId = "some-secret-id";

        Map<ConnectionPropertyKey, String> connectionProperties = new EnumMap<>(ConnectionPropertyKey.class);
        connectionProperties.put(ConnectionPropertyKey.JDBC_CONNECTION_URL, "connectionurl");
        connectionProperties.put(ConnectionPropertyKey.JDBC_DRIVER_CLASS_NAME, "driver");
        connectionProperties.put(ConnectionPropertyKey.SECRET_ID, secretId);

        JDBCCredentials credentials = new JDBCCredentials("user", null);

        when(mockGlueClient.getConnection(CONNECTION_NAME)).thenReturn(mockConnection);
        when(mockConnection.connectionProperties()).thenReturn(connectionProperties);
        when(mockSecretsManagerClient.getSecret(secretId, JDBCCredentials.class)).thenReturn(credentials);

        assertThrows(JDBCGlueConnectionDetailsException.class, () -> underTest.getConnectionDetails(CONNECTION_NAME));
    }
}
