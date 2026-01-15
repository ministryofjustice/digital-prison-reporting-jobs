package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.metrics.DisabledMetricReportingService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreServiceImpl;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreTransformation;
import uk.gov.justice.digital.provider.ConnectionPoolProvider;
import uk.gov.justice.digital.service.JDBCGlueConnectionDetailsService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreDataAccessService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreRepository;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import java.sql.Connection;
import java.util.Arrays;

import static org.apache.spark.sql.functions.lit;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.config.JobArguments.OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenDatastoreCredentials;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenEmptyTableExists;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenSchemaExists;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteToOperationalDataStore;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteToOperationalDataStoreTableNameIsConfigured;

@ExtendWith(MockitoExtension.class)
class BatchProcessorIT extends BaseMinimalDataIntegrationTest {
    protected static final InMemoryOperationalDataStore operationalDataStore = new InMemoryOperationalDataStore();
    private static Connection testQueryConnection;

    @Mock
    private JobArguments arguments;
    @Mock
    private JobProperties properties;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private ConfigService configService;
    @Mock
    private JDBCGlueConnectionDetailsService connectionDetailsService;

    private BatchProcessor underTest;

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
        givenDatastoreCredentials(connectionDetailsService, operationalDataStore);
        givenSettingsAreConfigured();
        givenSchemaExists(namespace, testQueryConnection);
        givenSchemaExists(configurationSchemaName, testQueryConnection);
        givenTablesToWriteToOperationalDataStoreTableNameIsConfigured(arguments, configurationSchemaName + "." + configurationTableName);
        givenTablesToWriteToOperationalDataStore(configurationSchemaName, configurationTableName, inputSchemaName, inputTableName, testQueryConnection);
        givenS3BatchProcessorDependenciesAreInjected();
        givenASourceReference();
    }

    @Test
    void shouldWriteInsertsToStructuredCuratedAndOperationalDataStore() throws Exception {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Update, "data3"),
                createRow(pk4, "2023-11-13 10:50:00.123456", Delete, "data4")
        ), TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);

        givenEmptyTableExists(operationalDataStoreFullTableName, input, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, input);

        thenStructuredCuratedAndOperationalDataStoreContainForPK("data1", pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK("data2", pk2, testQueryConnection);

        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk4, testQueryConnection);
    }

    @Test
    void shouldWriteNullsToViolationsForNonNullableColumns() throws Exception {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA);

        givenEmptyTableExists(operationalDataStoreFullTableName, input, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, input);

        thenStructuredCuratedAndOperationalDataStoreContainForPK("data1", pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK("data3", pk3, testQueryConnection);

        thenStructuredViolationsContainsForPK("data2", pk2);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
    }

    @Test
    void shouldWriteToViolationsForDfWithExtraColumn() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("extra-column", lit(1));

        givenEmptyTableExists(operationalDataStoreFullTableName, dfWithMisMatchingSchema, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    void shouldWriteToViolationsForDfWithMissingColumn() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).drop("data");

        givenEmptyTableExists(operationalDataStoreFullTableName, dfWithMisMatchingSchema, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    void shouldWriteToViolationsForDfWhenTypeGoesFromStringToInt() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1));

        givenEmptyTableExists(operationalDataStoreFullTableName, dfWithMisMatchingSchema, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    void shouldWriteToViolationsForDfWhenTypeGoesFromIntToString() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1));

        givenEmptyTableExists(operationalDataStoreFullTableName, dfWithMisMatchingSchema, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    void shouldWriteToViolationsForDfWhenTypeGoesFromIntToLong() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1L));

        givenEmptyTableExists(operationalDataStoreFullTableName, dfWithMisMatchingSchema, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    void shouldWriteToViolationsWhenSchemaChangesFromWhatIsAlreadyInViolations() throws Exception {
        // The 1st bad dataframe will be written to violations with one schema
        Dataset<Row> dfNullNonNullableCols = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA);

        givenEmptyTableExists(operationalDataStoreFullTableName, dfNullNonNullableCols, testQueryConnection, operationalDataStore);

        underTest.processBatch(spark, sourceReference, dfNullNonNullableCols);

        thenStructuredCuratedAndOperationalDataStoreContainForPK("data1", pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK("data3", pk3, testQueryConnection);

        thenStructuredViolationsContainsForPK("data2", pk2);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        // The 2nd bad dataframe will be written to violations with another, incompatible schema
        Dataset<Row> schemaChanged = spark.createDataFrame(Arrays.asList(
                        createRow(pk4, "2023-11-13 10:50:00.123456", Insert, "data1"),
                        createRow(pk5, null, Insert, "data2"),
                        createRow(pk6, "2023-11-13 10:50:00.123456", Insert, "data3")
                ), TEST_DATA_SCHEMA)
                .withColumn("data", lit(1))
                .withColumn("new-column", lit("new"));

        underTest.processBatch(spark, sourceReference, schemaChanged);

        thenStructuredViolationsContainsPK(pk4);
        thenStructuredViolationsContainsPK(pk5);
        thenStructuredViolationsContainsPK(pk6);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk4, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk5, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk6, testQueryConnection);
    }

    private void givenSettingsAreConfigured() {
        givenPathsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
        when(arguments.getOperationalDataStoreJdbcBatchSize()).thenReturn(OPERATIONAL_DATA_STORE_JDBC_BATCH_SIZE_DEFAULT);
        when(arguments.getOperationalDataStoreGlueConnectionName()).thenReturn("operational-datastore-connection");
        when(properties.getSparkDriverMemory()).thenReturn("2g");
        when(properties.getSparkExecutorMemory()).thenReturn("2g");
    }

    private void givenS3BatchProcessorDependenciesAreInjected() {
        DataStorageService storageService = new DataStorageService(arguments);
        S3DataProvider dataProvider = new S3DataProvider(arguments);
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments, configService);
        ViolationService violationService =
                new ViolationService(arguments, storageService, dataProvider, tableDiscoveryService, new DisabledMetricReportingService());
        ValidationService validationService = new ValidationService(violationService);
        StructuredZoneLoad structuredZoneLoad = new StructuredZoneLoad(arguments, storageService, violationService);
        CuratedZoneLoad curatedZoneLoad = new CuratedZoneLoad(arguments, storageService, violationService);
        OperationalDataStoreTransformation operationalDataStoreTransformation = new OperationalDataStoreTransformation();
        ConnectionPoolProvider connectionPoolProvider = new ConnectionPoolProvider();
        OperationalDataStoreRepository operationalDataStoreRepository =
                new OperationalDataStoreRepository(arguments, properties, connectionDetailsService, sparkSessionProvider);
        OperationalDataStoreDataAccessService operationalDataStoreDataAccessService =
                new OperationalDataStoreDataAccessService(arguments, connectionDetailsService, connectionPoolProvider, operationalDataStoreRepository);
        OperationalDataStoreService operationalDataStoreService =
                new OperationalDataStoreServiceImpl(arguments, operationalDataStoreTransformation, operationalDataStoreDataAccessService);
        underTest = new BatchProcessor(structuredZoneLoad, curatedZoneLoad, validationService, operationalDataStoreService);
    }

    private void givenPathsAreConfigured() {
        rawPath = testRoot.resolve("raw").toAbsolutePath().toString();
        structuredPath = testRoot.resolve("structured").toAbsolutePath().toString();
        curatedPath = testRoot.resolve("curated").toAbsolutePath().toString();
        violationsPath = testRoot.resolve("violations").toAbsolutePath().toString();
        when(arguments.getStructuredS3Path()).thenReturn(structuredPath);
        when(arguments.getCuratedS3Path()).thenReturn(curatedPath);
        lenient().when(arguments.getViolationsS3Path()).thenReturn(violationsPath);
    }

    private void givenASourceReference() {
        when(sourceReference.getNamespace()).thenReturn(namespace);
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
        when(sourceReference.getOperationalDataStoreTableName()).thenReturn(operationalDataStoreTableName);
        when(sourceReference.getFullOperationalDataStoreTableNameWithSchema()).thenReturn(operationalDataStoreFullTableName);
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
    }
}
