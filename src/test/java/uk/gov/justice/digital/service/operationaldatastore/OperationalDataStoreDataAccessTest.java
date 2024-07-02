package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreDataAccessTest {
    @Mock
    private OperationalDataStoreConnectionDetailsService connectionDetailsService;
    @Mock
    private Dataset<Row> dataframe;
    @Mock
    private DataFrameWriter<Row> dataframeWriter;
    @Mock
    private ConnectionPoolProvider connectionPoolProvider;
    @Mock
    private DataSource dataSource;
    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private StructType schema;
    @Mock
    private SourceReference.PrimaryKey primaryKey;

    private OperationalDataStoreDataAccess underTest;

    @Test
    void shouldRetrieveConnectionDetailsInConstructor() {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);

        verify(connectionDetailsService, times(1)).getConnectionDetails();
    }

    @Test
    void shouldInitialiseConnectionPoolInConstructor() {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);

        verify(connectionPoolProvider, times(1)).getConnectionPool(
                "jdbc-url",
                "org.postgresql.Driver",
                "username",
                "password"
        );
    }

    @Test
    void shouldOverwriteExistingTable() {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        String destinationTableName = "some.table";
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);
        when(dataframe.write()).thenReturn(dataframeWriter);
        when(dataframeWriter.mode(any(SaveMode.class))).thenReturn(dataframeWriter);

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);
        underTest.overwriteTable(dataframe, destinationTableName);

        Properties expectedProperties = new Properties();
        expectedProperties.put("user", "username");
        expectedProperties.put("password", "password");
        expectedProperties.put("driver", "org.postgresql.Driver");

        verify(dataframeWriter, times(1)).mode(SaveMode.Overwrite);
        verify(dataframeWriter, times(1))
                .jdbc("jdbc-url", destinationTableName, expectedProperties);
    }

    @Test
    void shouldMerge() throws Exception {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);
        when(connectionPoolProvider.getConnectionPool(any(), any(), any(), any())).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(schema.fieldNames()).thenReturn(new String[]{"column1", "column2"});
        when(primaryKey.getSparkCondition(any(), any())).thenReturn("s.column1 = d.column1");
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList("column1"));

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);
        underTest.merge(temporaryTableName, destinationTableName, sourceReference);

        verify(statement, times(2)).execute(any());
    }

    @Test
    void shouldCloseResources() throws Exception {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);
        when(connectionPoolProvider.getConnectionPool(any(), any(), any(), any())).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(schema.fieldNames()).thenReturn(new String[]{"column1", "column2"});
        when(primaryKey.getSparkCondition(any(), any())).thenReturn("s.column1 = d.column1");
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList("column1"));

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);
        underTest.merge(temporaryTableName, destinationTableName, sourceReference);

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

    @Test
    void shouldCloseResourcesWhenSqlExecutionThrows() throws Exception {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);
        when(connectionPoolProvider.getConnectionPool(any(), any(), any(), any())).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(any())).thenThrow(new SQLException());
        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(schema.fieldNames()).thenReturn(new String[]{"column1", "column2"});
        when(primaryKey.getSparkCondition(any(), any())).thenReturn("s.column1 = d.column1");
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList("column1"));

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);
        assertThrows(RuntimeException.class, () -> {
            underTest.merge(temporaryTableName, destinationTableName, sourceReference);
        });

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }


    @Test
    void shouldBuildMergeSql() {
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";

        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername("username");
        credentials.setPassword("password");
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );

        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);

        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(schema.fieldNames()).thenReturn(new String[]{"pk_col", "column2"});
        when(primaryKey.getSparkCondition(any(), any())).thenReturn("s.pk_col = d.pk_col");
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList("pk_col"));

        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider);

        String resultSql = underTest.buildMergeSql(temporaryTableName, destinationTableName, sourceReference);
        System.out.println(resultSql);
        String expectedSql = "MERGE INTO some.table d\n" +
                "USING loading.table s ON s.pk_col = d.pk_col\n" +
                "    WHEN MATCHED AND s.op = 'D' THEN DELETE\n" +
                "    WHEN MATCHED AND s.op = 'U' THEN UPDATE SET column2 = s.column2\n" +
                "    WHEN NOT MATCHED AND (s.op = 'I' OR s.op = 'U') THEN INSERT (pk_col, column2) VALUES (s.pk_col, s.column2)";
        assertEquals(resultSql, expectedSql);

    }

}