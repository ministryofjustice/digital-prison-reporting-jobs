package uk.gov.justice.digital.test;

import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreConnectionDetailsService;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;

public class SharedTestFunctions {

    public static void givenDatastoreCredentials(OperationalDataStoreConnectionDetailsService connectionDetailsService, InMemoryOperationalDataStore operationalDataStore) {
        when(connectionDetailsService.getConnectionDetails()).thenReturn(operationalDataStore.getConnectionDetails());
    }

    public static void givenSchemaExists(String schemaName, Connection testQueryConnection) throws SQLException {
        try(Statement statement = testQueryConnection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
    }

    public static void givenTablesToWriteToOperationalDataStoreTableNameIsConfigured(JobArguments arguments, String fullTableName) {
        when(arguments.getOperationalDataStoreTablesToWriteTableName()).thenReturn(fullTableName);
    }

    public static void givenTablesToWriteToOperationalDataStore(String configSchema, String configTable, String schemaName, String tableName, Connection testQueryConnection) throws SQLException {
        try(Statement statement = testQueryConnection.createStatement()) {
            statement.execute(format("CREATE TABLE IF NOT EXISTS %s.%s (source VARCHAR, table_name VARCHAR)", configSchema, configTable));
            statement.execute(format("INSERT INTO %s.%s VALUES ('%s', '%s')", configSchema, configTable, schemaName, tableName));
        }
    }

    public static void assertOperationalDataStoreContainsForPK(String schemaName, String table, String data, int primaryKey, Connection testQueryConnection) throws SQLException {
        String sql = format("SELECT COUNT(1) AS cnt FROM %s.%s WHERE %s = %d AND %s = '%s'",
                schemaName, table, PRIMARY_KEY_COLUMN, primaryKey, DATA_COLUMN, data);
        try(Statement statement = testQueryConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            if(resultSet.next()) {
                int count = resultSet.getInt(1);
                assertEquals(1, count);
            }
        }
    }

    public static void assertOperationalDataStoreDoesNotContainPK(String schemaName, String table, int primaryKey, Connection testQueryConnection) throws SQLException {
        try {
            String sql = format("SELECT COUNT(1) AS cnt FROM %s.%s WHERE %s = %d",
                    schemaName, table, PRIMARY_KEY_COLUMN, primaryKey);
            try (Statement statement = testQueryConnection.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);
                if (resultSet.next()) {
                    int count = resultSet.getInt(1);
                    assertEquals(0, count);
                }
            }
        } catch (SQLException e) {
            // If the table doesn't exist then that is fine and it doesn't contain the primary key
            if(!(e.getMessage().contains("Table") && e.getMessage().contains("not found"))) {
                throw e;
            }
        }
    }
}
