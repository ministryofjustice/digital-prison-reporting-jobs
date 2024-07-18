package uk.gov.justice.digital.service.operationaldatastore;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.ConnectionPoolProvider;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreConnectionDetailsService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreDataAccess;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreRepositoryProvider;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenDataHubManagedTableIsConfigured;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenDatastoreCredentials;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenSchemaExists;

@ExtendWith(MockitoExtension.class)
public class OperationalDataStoreServiceIntegrationTest extends BaseSparkTest {

    private static final InMemoryOperationalDataStore operationalDataStore = new InMemoryOperationalDataStore();
    private static Connection testQueryConnection;

    private static final StructType schema = new StructType(new StructField[]{
            new StructField("PK", DataTypes.StringType, true, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()),
            new StructField("DATA", DataTypes.StringType, true, Metadata.empty())
    });

    @Mock
    private OperationalDataStoreConnectionDetailsService mockConnectionDetailsService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private JobArguments jobArguments;

    private OperationalDataStoreService underTest;

    private String destinationTableNameWithSchema;
    private Properties jdbcProperties;

    @BeforeAll
    static void beforeAll() throws Exception {
        operationalDataStore.start();
        testQueryConnection = operationalDataStore.getJdbcConnection();
    }

    @AfterAll
    static void afterAll() throws Exception {
        testQueryConnection.close();
        operationalDataStore.stop();
    }

    @BeforeEach
    void setUp() throws Exception {
        givenDatastoreCredentials(mockConnectionDetailsService, operationalDataStore);

        jdbcProperties = new Properties();
        jdbcProperties.put("user", operationalDataStore.getUsername());
        jdbcProperties.put("password", operationalDataStore.getPassword());

        // Use unique tables for each test.
        // Postgres table names cannot start with a number, hence the underscore prefix, and cannot contain hyphens/dashes.
        String configurationSchema = "configuration";
        String inputSchemaName = "nomis";
        String loadingSchemaName = "loading";
        String inputTableName = "_" + UUID.randomUUID().toString().replaceAll("-", "_");
        destinationTableNameWithSchema = inputSchemaName + "." + inputTableName;
        String configurationTable = "datahub_managed_tables";
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(destinationTableNameWithSchema);
        when(jobArguments.getOperationalDataStoreLoadingSchemaName()).thenReturn(loadingSchemaName);

        givenSchemaExists(configurationSchema, testQueryConnection);
        givenSchemaExists(loadingSchemaName, testQueryConnection);
        givenSchemaExists(inputSchemaName, testQueryConnection);
        givenDataHubManagedTableIsConfigured(configurationSchema, configurationTable, inputSchemaName, inputTableName, testQueryConnection);
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);

        ConnectionPoolProvider connectionPoolProvider = new ConnectionPoolProvider();
        OperationalDataStoreRepositoryProvider operationalDataStoreRepositoryProvider = new OperationalDataStoreRepositoryProvider();
        underTest = new OperationalDataStoreServiceImpl(
                jobArguments,
                new OperationalDataStoreTransformation(),
                new OperationalDataStoreDataAccess(mockConnectionDetailsService, connectionPoolProvider, operationalDataStoreRepositoryProvider)
        );
    }

    @Test
    public void overwriteDataShouldInsertData() {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other data")
        ), schema);

        underTest.overwriteData(df, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertEquals(2, result.count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("pk").contains("pk2")).count());
        assertEquals(1, result.where(col("data").contains("some data")).count());
        assertEquals(1, result.where(col("data").contains("some other data")).count());
    }

    @Test
    public void overwriteDataShouldOverwriteExistingData() {
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other data")
        ), schema);

        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some new data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other new data")
        ), schema);

        underTest.overwriteData(df1, sourceReference);
        underTest.overwriteData(df2, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertEquals(2, result.count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("pk").contains("pk2")).count());
        assertEquals(1, result.where(col("data").contains("some new data")).count());
        assertEquals(1, result.where(col("data").contains("some other new data")).count());
        assertEquals(0, result.where(col("data").contains("some data")).count());
        assertEquals(0, result.where(col("data").contains("some other data")).count());
    }

    @Test
    public void overwriteDataShouldCreateTableWithLowercaseColumnsWithoutOpAndTimestamp() {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other data")
        ), schema);

        underTest.overwriteData(df, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertThat(result.columns(), arrayContainingInAnyOrder("pk", "data"));
        assertThat(result.columns(), not(arrayContainingInAnyOrder(OPERATION, OPERATION.toLowerCase(), TIMESTAMP)));
    }

    @Test
    public void mergeDataShouldInsertWhenOpIsInsertOrUpdateAndPkNotPresent() throws Exception {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "U", "some other data")
        ), schema);
        Dataset<Row> dfWithoutMetadataCols = df.drop(TIMESTAMP, OPERATION);
        StructType schemaWithoutMetadataCols = dfWithoutMetadataCols.schema();

        when(sourceReference.getSchema()).thenReturn(schemaWithoutMetadataCols);
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("PK"));

        // Create the empty table (in reality this is done by the batch job)
        createDestinationTable();

        underTest.mergeData(df, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertEquals(2, result.count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("pk").contains("pk2")).count());
        assertEquals(1, result.where(col("data").contains("some data")).count());
        assertEquals(1, result.where(col("data").contains("some other data")).count());
    }

    @Test
    public void mergeDataShouldUpdateWhenOpIsUpdateAndPkPresent() throws Exception {
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "initial data")
        ), schema);
        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "U", "updated data")
        ), schema);
        Dataset<Row> dfWithoutMetadataCols = df1.drop(TIMESTAMP, OPERATION);
        StructType schemaWithoutMetadataCols = dfWithoutMetadataCols.schema();

        when(sourceReference.getSchema()).thenReturn(schemaWithoutMetadataCols);
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("PK"));

        // Create the table with initial data
        createDestinationTable();
        underTest.mergeData(df1, sourceReference);
        // Check our test preconditions
        Dataset<Row> dataInTableBeforeMerge = retrieveAlDataInTable();
        assertEquals(1, dataInTableBeforeMerge.where(col("pk").contains("pk1")).count());
        assertEquals(1, dataInTableBeforeMerge.where(col("data").contains("initial data")).count());
        // Run the merge
        underTest.mergeData(df2, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertEquals(1, result.count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(0, result.where(col("data").contains("initial data")).count());
        assertEquals(1, result.where(col("data").contains("updated data")).count());
    }

    @Test
    public void mergeDataShouldDeleteWhenOpIsDeleteAndPkPresent() throws Exception {
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "initial data")
        ), schema);
        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "D", "updated data")
        ), schema);
        Dataset<Row> dfWithoutMetadataCols = df1.drop(TIMESTAMP, OPERATION);
        StructType schemaWithoutMetadataCols = dfWithoutMetadataCols.schema();

        when(sourceReference.getSchema()).thenReturn(schemaWithoutMetadataCols);
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("PK"));

        // Create the table with initial data
        createDestinationTable();
        underTest.mergeData(df1, sourceReference);
        // Check our test preconditions
        Dataset<Row> dataInTableBeforeMerge = retrieveAlDataInTable();
        assertEquals(1, dataInTableBeforeMerge.where(col("pk").contains("pk1")).count());
        assertEquals(1, dataInTableBeforeMerge.where(col("data").contains("initial data")).count());
        // Run the merge
        underTest.mergeData(df2, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertEquals(0, result.count());
    }

    @Test
    public void mergeDataShouldDoNothingWhenOpIsInsertAndPkIsPresent() throws Exception {
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "initial data")
        ), schema);
        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "updated data")
        ), schema);
        Dataset<Row> dfWithoutMetadataCols = df1.drop(TIMESTAMP, OPERATION);
        StructType schemaWithoutMetadataCols = dfWithoutMetadataCols.schema();

        when(sourceReference.getSchema()).thenReturn(schemaWithoutMetadataCols);
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("PK"));

        // Create the table with initial data
        createDestinationTable();
        underTest.mergeData(df1, sourceReference);
        // Check our test preconditions
        Dataset<Row> dataInTableBeforeMerge = retrieveAlDataInTable();
        assertEquals(1, dataInTableBeforeMerge.where(col("pk").contains("pk1")).count());
        assertEquals(1, dataInTableBeforeMerge.where(col("data").contains("initial data")).count());
        // Run the merge
        underTest.mergeData(df2, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertEquals(1, result.count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("data").contains("initial data")).count());
        assertEquals(0, result.where(col("data").contains("updated data")).count());
    }

    @Test
    public void mergeDataShouldDoNothingWhenOpIsDeleteAndPkNotPresent() throws Exception {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "D", "initial data")
        ), schema);
        Dataset<Row> dfWithoutMetadataCols = df.drop(TIMESTAMP, OPERATION);
        StructType schemaWithoutMetadataCols = dfWithoutMetadataCols.schema();

        when(sourceReference.getSchema()).thenReturn(schemaWithoutMetadataCols);
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey("PK"));

        // Create the table with initial data
        createDestinationTable();
        // Run the merge
        underTest.mergeData(df, sourceReference);

        Dataset<Row> result = retrieveAlDataInTable();

        assertEquals(0, result.count());
    }

    private void createDestinationTable() throws SQLException {
        try (Statement statement = testQueryConnection.createStatement()) {
            statement.execute(format("CREATE TABLE IF NOT EXISTS %s (pk VARCHAR, data VARCHAR)", destinationTableNameWithSchema));
        }
    }

    private Dataset<Row> retrieveAlDataInTable() {
        return spark.read().jdbc(operationalDataStore.getJdbcUrl(), destinationTableNameWithSchema, jdbcProperties);
    }
}
