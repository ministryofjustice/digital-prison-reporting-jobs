package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.DataHubOperationalDataStoreManagedTable;
import uk.gov.justice.digital.datahub.model.JDBCGlueConnectionDetails;
import uk.gov.justice.digital.datahub.model.JDBCCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.OperationalDataStoreException;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.config.JobArguments.OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreDataAccessServiceTest {
    private static final String NAMESPACE = "namespace";
    private static final String FULL_TABLE_NAME = "schema_name_table_name";
    private static final String GLUE_CONNECTION_NAME = "connection";

    private static final Set<DataHubOperationalDataStoreManagedTable> managedTables = new HashSet<>(Arrays.asList(
            new DataHubOperationalDataStoreManagedTable("nomis", "activities"),
            new DataHubOperationalDataStoreManagedTable("nomis", "prisoners"),
            new DataHubOperationalDataStoreManagedTable("nomis", "external_movements")
    ));
    @Mock
    private JobArguments jobArguments;
    @Mock
    private JDBCGlueConnectionDetailsService connectionDetailsService;
    @Mock
    private Dataset<Row> dataframe;
    @Mock
    private DataFrameWriter<Row> dataframeWriter;
    @Mock
    private ConnectionPoolProvider connectionPoolProvider;
    @Mock
    private OperationalDataStoreRepository operationalDataStoreRepository;
    @Mock
    private DataSource dataSource;
    @Mock
    private Connection connection;
    @Mock
    private Statement statement;
    @Mock
    private ResultSet resultSet;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private StructType schema;
    @Mock
    private SourceReference.PrimaryKey primaryKey;

    private OperationalDataStoreDataAccessService underTest;

    @BeforeEach
    void setup() {
        JDBCCredentials credentials = new JDBCCredentials("username", "password");
        JDBCGlueConnectionDetails connectionDetails = new JDBCGlueConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );
        when(jobArguments.getOperationalDataStoreGlueConnectionName()).thenReturn(GLUE_CONNECTION_NAME);
        when(connectionDetailsService.getConnectionDetails(GLUE_CONNECTION_NAME)).thenReturn(connectionDetails);
        when(connectionPoolProvider.getConnectionPool(any(), any(), any(), any())).thenReturn(dataSource);
        when(operationalDataStoreRepository.getDataHubOperationalDataStoreManagedTables()).thenReturn(managedTables);
        underTest = new OperationalDataStoreDataAccessService(jobArguments, connectionDetailsService, connectionPoolProvider, operationalDataStoreRepository);
    }

    @Test
    void shouldRetrieveConnectionDetailsInConstructor() {
        verify(connectionDetailsService, times(1)).getConnectionDetails(GLUE_CONNECTION_NAME);
    }

    @Test
    void shouldInitialiseConnectionPoolInConstructor() {
        verify(connectionPoolProvider, times(1)).getConnectionPool(
                "jdbc-url",
                "org.postgresql.Driver",
                "username",
                "password"
        );
    }

    @Test
    void shouldRetrieveManagedTablesInConstructor() {
        verify(operationalDataStoreRepository, times(1)).getDataHubOperationalDataStoreManagedTables();
    }

    @Test
    void shouldOverwriteExistingTable() {
        String destinationTableName = "some.table";
        boolean truncate = true;

        when(dataframe.write()).thenReturn(dataframeWriter);
        when(dataframeWriter.mode(any(SaveMode.class))).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), any())).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), anyLong())).thenReturn(dataframeWriter);
        when(jobArguments.getOperationalDataStoreJdbcBatchSize()).thenReturn(OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT);

        underTest.overwriteTable(dataframe, destinationTableName, truncate);

        Properties expectedProperties = new Properties();
        expectedProperties.put("user", "username");
        expectedProperties.put("password", "password");
        expectedProperties.put("driver", "org.postgresql.Driver");

        verify(dataframeWriter, times(1)).mode(SaveMode.Overwrite);
        verify(dataframeWriter, times(1))
                .jdbc("jdbc-url", destinationTableName, expectedProperties);
    }

    @Test
    void overwriteTableShouldTruncateRatherThanDrop() {
        String destinationTableName = "some.table";

        when(dataframe.write()).thenReturn(dataframeWriter);
        when(dataframeWriter.mode(any(SaveMode.class))).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), any())).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), anyLong())).thenReturn(dataframeWriter);
        when(jobArguments.getOperationalDataStoreJdbcBatchSize()).thenReturn(OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT);

        boolean truncate = true;
        underTest.overwriteTable(dataframe, destinationTableName, truncate);

        verify(dataframeWriter, times(1)).option("truncate", true);
    }

    @Test
    void overwriteTableShouldDropRatherThanTruncate() {
        String destinationTableName = "some.table";

        when(dataframe.write()).thenReturn(dataframeWriter);
        when(dataframeWriter.mode(any(SaveMode.class))).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), any())).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), anyLong())).thenReturn(dataframeWriter);
        when(jobArguments.getOperationalDataStoreJdbcBatchSize()).thenReturn(OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT);

        boolean truncate = false;
        underTest.overwriteTable(dataframe, destinationTableName, truncate);

        verify(dataframeWriter, times(1)).option("truncate", false);
    }

    @Test
    void overwriteTableShouldUseJdbcBatchSize() {
        String destinationTableName = "some.table";
        boolean truncate = true;
        when(dataframe.write()).thenReturn(dataframeWriter);
        when(dataframeWriter.mode(any(SaveMode.class))).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), any())).thenReturn(dataframeWriter);
        when(dataframeWriter.option(any(), anyLong())).thenReturn(dataframeWriter);
        when(jobArguments.getOperationalDataStoreJdbcBatchSize()).thenReturn(5000L);

        underTest.overwriteTable(dataframe, destinationTableName, truncate);

        verify(dataframeWriter, times(1)).option("batchSize", 5000L);
    }

    @Test
    void shouldMerge() throws Exception {
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(schema.fieldNames()).thenReturn(new String[]{"column1", "column2"});
        when(primaryKey.getSparkCondition(any(), any())).thenReturn("s.column1 = d.column1");
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList("column1"));

        underTest.merge(temporaryTableName, destinationTableName, sourceReference);

        verify(statement, times(2)).execute(any());
    }

    @Test
    void mergeShouldCloseResources() throws Exception {
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(schema.fieldNames()).thenReturn(new String[]{"column1", "column2"});
        when(primaryKey.getSparkCondition(any(), any())).thenReturn("s.column1 = d.column1");
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList("column1"));

        underTest.merge(temporaryTableName, destinationTableName, sourceReference);

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

    @Test
    void mergeShouldCloseResourcesWhenSqlExecutionThrows() throws Exception {
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.execute(any())).thenThrow(new SQLException());
        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(primaryKey);
        when(schema.fieldNames()).thenReturn(new String[]{"column1", "column2"});
        when(primaryKey.getSparkCondition(any(), any())).thenReturn("s.column1 = d.column1");
        when(primaryKey.getKeyColumnNames()).thenReturn(Arrays.asList("column1"));

        assertThrows(OperationalDataStoreException.class, () -> {
            underTest.merge(temporaryTableName, destinationTableName, sourceReference);
        });

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }


    @Test
    void shouldBuildMergeSql() {
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";

        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("pk_col"));
        when(schema.fieldNames()).thenReturn(new String[]{"pk_col", "column2"});

        String resultSql = underTest.buildMergeSql(temporaryTableName, destinationTableName, sourceReference);
        System.out.println(resultSql);
        String expectedSql = "MERGE INTO some.table destination\n" +
                "USING loading.table source ON source.pk_col = destination.pk_col\n" +
                "    WHEN MATCHED AND source.op = 'D' THEN DELETE\n" +
                "    WHEN MATCHED AND source.op = 'U' THEN UPDATE SET column2 = source.column2\n" +
                "    WHEN NOT MATCHED AND (source.op = 'I' OR source.op = 'U') THEN INSERT (pk_col, column2) VALUES (source.pk_col, source.column2)";
        assertEquals(expectedSql, resultSql);
    }

    @Test
    void shouldOmitUpdateClauseForTablesWithAllPrimaryKeyColumns() {
        String temporaryTableName = "loading.table";
        String destinationTableName = "some.table";

        when(sourceReference.getSchema()).thenReturn(schema);
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey(Arrays.asList("pk_col1", "pk_col2")));
        when(schema.fieldNames()).thenReturn(new String[]{"pk_col1", "pk_col2"});

        String resultSql = underTest.buildMergeSql(temporaryTableName, destinationTableName, sourceReference);
        System.out.println(resultSql);
        String expectedSql = "MERGE INTO some.table destination\n" +
                "USING loading.table source ON source.pk_col1 = destination.pk_col1 and source.pk_col2 = destination.pk_col2\n" +
                "    WHEN MATCHED AND source.op = 'D' THEN DELETE\n" +
                "    WHEN NOT MATCHED AND (source.op = 'I' OR source.op = 'U') THEN INSERT (pk_col1, pk_col2) VALUES (source.pk_col1, source.pk_col2)";
        assertEquals(expectedSql, resultSql);
    }

    @Test
    void isOperationalDataStoreManagedTableShouldReturnTrueForManagedTables() {
        when(sourceReference.getSource()).thenReturn("nomis");
        when(sourceReference.getTable()).thenReturn("activities");
        assertTrue(underTest.isOperationalDataStoreManagedTable(sourceReference));
    }

    @Test
    void isOperationalDataStoreManagedTableShouldReturnFalseForNonManagedSource() {
        when(sourceReference.getSource()).thenReturn("source_not_in_managed_tables");
        when(sourceReference.getTable()).thenReturn("activities");
        assertFalse(underTest.isOperationalDataStoreManagedTable(sourceReference));
    }

    @Test
    void isOperationalDataStoreManagedTableShouldReturnFalseForNonManagedTable() {
        when(sourceReference.getSource()).thenReturn("nomis");
        when(sourceReference.getTable()).thenReturn("table_not_in_managed_tables");
        assertFalse(underTest.isOperationalDataStoreManagedTable(sourceReference));
    }

    @Test
    void shouldReturnTrueIfTableExists() throws Exception {
        when(sourceReference.getNamespace()).thenReturn(NAMESPACE);
        when(sourceReference.getOperationalDataStoreTableName()).thenReturn(FULL_TABLE_NAME);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(anyInt())).thenReturn(true);

        assertTrue(underTest.tableExists(sourceReference));
    }

    @Test
    void shouldReturnFalseIfTableDoesNotExist() throws Exception {
        when(sourceReference.getNamespace()).thenReturn(NAMESPACE);
        when(sourceReference.getOperationalDataStoreTableName()).thenReturn(FULL_TABLE_NAME);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(anyInt())).thenReturn(false);

        assertFalse(underTest.tableExists(sourceReference));
    }

    @Test
    void shouldReturnFalseIfNoResultsForQuery() throws Exception {
        when(sourceReference.getNamespace()).thenReturn(NAMESPACE);
        when(sourceReference.getOperationalDataStoreTableName()).thenReturn(FULL_TABLE_NAME);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);

        assertFalse(underTest.tableExists(sourceReference));
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

        assertThrows(OperationalDataStoreException.class, () -> {
            underTest.getTableRowCount("some_schema.some_table");
        });

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

    @Test
    void shouldRunSqlToCheckIfTableExists() throws Exception {
        when(sourceReference.getNamespace()).thenReturn(NAMESPACE);
        when(sourceReference.getOperationalDataStoreTableName()).thenReturn(FULL_TABLE_NAME);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(anyInt())).thenReturn(true);

        underTest.tableExists(sourceReference);

        String expectedSql = "SELECT EXISTS (SELECT  FROM information_schema.tables WHERE table_schema = 'namespace' AND table_name = 'schema_name_table_name')";
        verify(statement, times(1)).executeQuery(expectedSql);
        verify(resultSet, times(1)).getBoolean(1);
    }

    @Test
    void tableExistsShouldCloseResources() throws Exception {
        when(sourceReference.getNamespace()).thenReturn(NAMESPACE);
        when(sourceReference.getOperationalDataStoreTableName()).thenReturn(FULL_TABLE_NAME);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getBoolean(anyInt())).thenReturn(true);

        underTest.tableExists(sourceReference);

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

    @Test
    void tableExistsShouldCloseResourcesWhenSqlExecutionThrows() throws Exception {
        when(sourceReference.getNamespace()).thenReturn(NAMESPACE);
        when(sourceReference.getOperationalDataStoreTableName()).thenReturn(FULL_TABLE_NAME);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(any())).thenThrow(new SQLException());

        assertThrows(OperationalDataStoreException.class, () -> {
            underTest.tableExists(sourceReference);
        });

        verify(connection, times(1)).close();
        verify(statement, times(1)).close();
    }

}
