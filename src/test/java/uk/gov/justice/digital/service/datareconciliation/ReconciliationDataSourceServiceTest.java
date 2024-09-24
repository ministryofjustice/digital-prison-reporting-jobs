package uk.gov.justice.digital.service.datareconciliation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.JDBCCredentials;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;
import uk.gov.justice.digital.exception.ReconciliationDataSourceException;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;

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
class ReconciliationDataSourceServiceTest {

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

    private ReconciliationDataSourceService underTest;

    @BeforeEach
    public void setup() {
        JDBCCredentials credentials = new JDBCCredentials("username", "password");
        JDBCGlueConnectionDetails connectionDetails = new JDBCGlueConnectionDetails(
                "jdbc-url", "some-driver-class", credentials
        );
        when(jobArguments.getReconciliationDataSourceGlueConnectionName()).thenReturn(GLUE_CONNECTION_NAME);
        when(connectionDetailsService.getConnectionDetails(GLUE_CONNECTION_NAME)).thenReturn(connectionDetails);
        when(connectionPoolProvider.getConnectionPool(any(), any(), any(), any())).thenReturn(dataSource);
    }

    @Test
    void shouldRetrieveConnectionDetailsInConstructor() {
        underTest = new ReconciliationDataSourceService(jobArguments, connectionPoolProvider, connectionDetailsService);
        verify(connectionDetailsService, times(1)).getConnectionDetails(GLUE_CONNECTION_NAME);
    }

    @Test
    void shouldInitialiseConnectionPoolInConstructor() {
        underTest = new ReconciliationDataSourceService(jobArguments, connectionPoolProvider, connectionDetailsService);
        verify(connectionPoolProvider, times(1)).getConnectionPool(
                "jdbc-url",
                "some-driver-class",
                "username",
                "password"
        );
    }

    @Test
    void shouldGetTableRowCount() throws Exception {
        underTest = new ReconciliationDataSourceService(jobArguments, connectionPoolProvider, connectionDetailsService);
        long count = 9999L;

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(count);

        long result = underTest.getTableRowCount("some_table");
        assertEquals(count, result);
    }

    @Test
    void getTableRowCountShouldQueryWithTheSchema() throws Exception {
        when(jobArguments.getReconciliationDataSourceSourceSchemaName()).thenReturn("my_schema");
        when(jobArguments.shouldReconciliationDataSourceTableNamesBeUpperCase()).thenReturn(false);

        underTest = new ReconciliationDataSourceService(jobArguments, connectionPoolProvider, connectionDetailsService);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(1L);

        underTest.getTableRowCount("some_table");
        String expectedSql = "SELECT COUNT(1) FROM my_schema.some_table";
        verify(statement, times(1)).executeQuery(expectedSql);
    }

    @Test
    void getTableRowCountShouldUppercaseTableNameWhenConfiguredTo() throws Exception {
        when(jobArguments.getReconciliationDataSourceSourceSchemaName()).thenReturn("my_schema");
        when(jobArguments.shouldReconciliationDataSourceTableNamesBeUpperCase()).thenReturn(true);

        underTest = new ReconciliationDataSourceService(jobArguments, connectionPoolProvider, connectionDetailsService);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(1)).thenReturn(1L);

        underTest.getTableRowCount("some_table");
        String expectedSql = "SELECT COUNT(1) FROM MY_SCHEMA.SOME_TABLE";
        verify(statement, times(1)).executeQuery(expectedSql);
    }

    @Test
    void getTableRowCountShouldCloseResources() throws Exception {
        underTest = new ReconciliationDataSourceService(jobArguments, connectionPoolProvider, connectionDetailsService);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getLong(anyInt())).thenReturn(1L);

        underTest.getTableRowCount("some_table");

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

    @Test
    void getTableRowCountShouldCloseResourcesWhenSqlExecutionThrows() throws Exception {
        underTest = new ReconciliationDataSourceService(jobArguments, connectionPoolProvider, connectionDetailsService);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenThrow(new SQLException());

        assertThrows(ReconciliationDataSourceException.class, () -> {
            underTest.getTableRowCount("some_table");
        });

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }
}