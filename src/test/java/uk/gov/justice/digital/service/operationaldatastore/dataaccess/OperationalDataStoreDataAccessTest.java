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
import uk.gov.justice.digital.datahub.model.DataHubOperationalDataStoreManagedTable;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.OperationalDataStoreException;

import javax.sql.DataSource;
import java.sql.Connection;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OperationalDataStoreDataAccessTest {

    private static final Set<DataHubOperationalDataStoreManagedTable> managedTables = new HashSet<>(Arrays.asList(
            new DataHubOperationalDataStoreManagedTable("nomis", "activities"),
            new DataHubOperationalDataStoreManagedTable("nomis", "prisoners"),
            new DataHubOperationalDataStoreManagedTable("nomis", "external_movements")
    ));
    @Mock
    private OperationalDataStoreConnectionDetailsService connectionDetailsService;
    @Mock
    private Dataset<Row> dataframe;
    @Mock
    private DataFrameWriter<Row> dataframeWriter;
    @Mock
    private ConnectionPoolProvider connectionPoolProvider;
    @Mock
    private OperationalDataStoreRepositoryProvider operationalDataStoreRepositoryProvider;
    @Mock
    private OperationalDataStoreRepository operationalDataStoreRepository;
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

    @BeforeEach
    public void setup() {
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials("username", "password");
        OperationalDataStoreConnectionDetails connectionDetails = new OperationalDataStoreConnectionDetails(
                "jdbc-url", "org.postgresql.Driver", credentials
        );
        when(connectionDetailsService.getConnectionDetails()).thenReturn(connectionDetails);
        when(connectionPoolProvider.getConnectionPool(any(), any(), any(), any())).thenReturn(dataSource);
        when(operationalDataStoreRepositoryProvider.getOperationalDataStoreRepository(any())).thenReturn(operationalDataStoreRepository);
        when(operationalDataStoreRepository.getDataHubOperationalDataStoreManagedTables()).thenReturn(managedTables);
        underTest = new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider, operationalDataStoreRepositoryProvider);
    }

    @Test
    void shouldRetrieveConnectionDetailsInConstructor() {
        verify(connectionDetailsService, times(1)).getConnectionDetails();
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

        when(dataframe.write()).thenReturn(dataframeWriter);
        when(dataframeWriter.mode(any(SaveMode.class))).thenReturn(dataframeWriter);

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
}