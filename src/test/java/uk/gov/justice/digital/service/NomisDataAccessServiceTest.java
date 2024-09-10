package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.JDBCCredentials;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;
import uk.gov.justice.digital.exception.NomisDataAccessException;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NomisDataAccessServiceTest {

    private static final String GLUE_CONNECTION_NAME = "connection";

    @Mock
    private JobArguments jobArguments;
    @Mock
    private ConnectionPoolProvider connectionPoolProvider;
    @Mock
    private JDBCGlueConnectionDetailsService connectionDetailsService;
    @Mock
    private DataSource dataSource;
    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    @Mock
    private ResultSet resultSet;

    private NomisDataAccessService underTest;

    @BeforeEach
    public void setup() {
        JDBCCredentials credentials = new JDBCCredentials("username", "password");
        JDBCGlueConnectionDetails connectionDetails = new JDBCGlueConnectionDetails(
                "jdbc-url", "some-driver-class", credentials
        );
        when(jobArguments.getNomisGlueConnectionName()).thenReturn(GLUE_CONNECTION_NAME);
        when(connectionDetailsService.getConnectionDetails(GLUE_CONNECTION_NAME)).thenReturn(connectionDetails);
        when(connectionPoolProvider.getConnectionPool(any(), any(), any(), any())).thenReturn(dataSource);

        underTest = new NomisDataAccessService(jobArguments, connectionPoolProvider, connectionDetailsService);
    }

    @Test
    void shouldRetrieveConnectionDetailsInConstructor() {
        verify(connectionDetailsService, times(1)).getConnectionDetails(GLUE_CONNECTION_NAME);
    }

    @Test
    void shouldInitialiseConnectionPoolInConstructor() {
        verify(connectionPoolProvider, times(1)).getConnectionPool(
                "jdbc-url",
                "some-driver-class",
                "username",
                "password"
        );
    }

    @Test
    void shouldGetTableRowCount() throws Exception {
        long count = 9999L;

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(count);

        long result = underTest.getTableRowCount("some_schema.some_table");
        assertEquals(count, result);
    }

    @Test
    void getTableRowCountShouldCloseResources() throws Exception {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(anyInt())).thenReturn(1L);

        underTest.getTableRowCount("some_schema.some_table");

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

    @Test
    void getTableRowCountShouldCloseResourcesWhenSqlExecutionThrows() throws Exception {
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenThrow(new SQLException());

        assertThrows(NomisDataAccessException.class, () -> {
            underTest.getTableRowCount("some_schema.some_table");
        });

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }
}