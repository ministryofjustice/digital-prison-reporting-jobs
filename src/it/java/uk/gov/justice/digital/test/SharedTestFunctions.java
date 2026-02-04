package uk.gov.justice.digital.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.DATA_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;

public class SharedTestFunctions {

    public static String operationalDataStoreTableName(String inputSchemaName, String tableName) {
        return format("%s_%s", inputSchemaName, tableName);
    }

    public static String operationalDataStoreTableNameWithSchema(String namespace, String inputSchemaName, String tableName) {
        return format("%s.%s", namespace, operationalDataStoreTableName(inputSchemaName, tableName));
    }

    public static void givenDatastoreCredentials(JDBCGlueConnectionDetailsService connectionDetailsService, InMemoryOperationalDataStore operationalDataStore) {
        when(connectionDetailsService.getConnectionDetails(anyString())).thenReturn(operationalDataStore.getConnectionDetails());
    }

    public static void givenSchemaExists(String schemaName, Connection testQueryConnection) throws SQLException {
        try (Statement statement = testQueryConnection.createStatement()) {
            statement.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
    }

    public static void givenEmptyTableExists(String fullTableName, Dataset<Row> df, Connection testQueryConnection, InMemoryOperationalDataStore operationalDataStore) throws SQLException {
        df.write().mode(SaveMode.Overwrite).jdbc(operationalDataStore.getJdbcUrl(), fullTableName, operationalDataStore.getJdbcProperties());
        try (Statement statement = testQueryConnection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + fullTableName);
        }
    }

    public static void givenTablesToWriteToOperationalDataStoreTableNameIsConfigured(JobArguments arguments, String fullTableName) {
        when(arguments.getOperationalDataStoreTablesToWriteTableName()).thenReturn(fullTableName);
    }

    public static void givenTablesToWriteToOperationalDataStore(String configSchema, String configTable, String schemaName, String tableName, Connection testQueryConnection) throws SQLException {
        try (Statement statement = testQueryConnection.createStatement()) {
            statement.execute(format("CREATE TABLE IF NOT EXISTS %s.%s (source VARCHAR, table_name VARCHAR)", configSchema, configTable));
            statement.execute(format("INSERT INTO %s.%s VALUES ('%s', '%s')", configSchema, configTable, schemaName, tableName));
        }
    }

    public static void assertOperationalDataStoreContainsForPK(String schemaName, String table, String data, int primaryKey, Connection testQueryConnection) throws SQLException {
        String sql = format("SELECT COUNT(1) AS cnt FROM %s.%s WHERE %s = %d AND %s = '%s'",
                schemaName, table, PRIMARY_KEY_COLUMN, primaryKey, DATA_COLUMN, data);
        try (Statement statement = testQueryConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(sql);
            if (resultSet.next()) {
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
            if (!(e.getMessage().contains("Table") && e.getMessage().contains("not found"))) {
                throw e;
            }
        }
    }

    @FunctionalInterface
    public interface Thunk {
        void apply() throws Exception;
    }

    public static void thenEventually(Thunk thunk) throws Throwable {
        Optional<Throwable> maybeEx = Optional.empty();
        for (int i = 0; i < 30; i++) {
            try {
                thunk.apply();
                maybeEx = Optional.empty();
                break;
            } catch (Exception | AssertionError e) {
                maybeEx = Optional.of(e);
                TimeUnit.SECONDS.sleep(2);
            }
        }
        if(maybeEx.isPresent()) {
            throw maybeEx.get();
        }
    }
}
