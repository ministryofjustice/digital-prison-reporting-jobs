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
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.TableDiscoveryService;
import uk.gov.justice.digital.service.ValidationService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.ConnectionPoolProvider;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreConnectionDetailsService;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreDataAccess;
import uk.gov.justice.digital.service.operationaldatastore.dataaccess.OperationalDataStoreRepositoryProvider;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreServiceImpl;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreTransformation;
import uk.gov.justice.digital.test.BaseMinimalDataIntegrationTest;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import java.sql.Connection;
import java.util.Arrays;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.lit;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteContains;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenDatastoreCredentials;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenSchemaExists;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteTableNameIsConfigured;

@ExtendWith(MockitoExtension.class)
class BatchProcessorIT extends BaseMinimalDataIntegrationTest {
    protected static final InMemoryOperationalDataStore operationalDataStore = new InMemoryOperationalDataStore();
    private static Connection testQueryConnection;

    @Mock
    private JobArguments arguments;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private ConfigService configService;
    @Mock
    private OperationalDataStoreConnectionDetailsService connectionDetailsService;

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
    public void setUp() throws Exception {
        givenDatastoreCredentials(connectionDetailsService, operationalDataStore);
        givenPathsAreConfigured();
        givenRetrySettingsAreConfigured(arguments);
        givenSchemaExists(inputSchemaName, testQueryConnection);
        givenSchemaExists(configurationSchemaName, testQueryConnection);
        givenTablesToWriteTableNameIsConfigured(arguments, configurationSchemaName + "." + configurationTableName);
        givenTablesToWriteContains(configurationSchemaName, configurationTableName, inputSchemaName, inputTableName, testQueryConnection);
        givenS3BatchProcessorDependenciesAreInjected();
        givenASourceReference();
    }

    @Test
    public void shouldWriteInsertsToStructuredCuratedAndOperationalDataStore() throws Exception {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, "2023-11-13 10:50:00.123456", Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Update, "data3"),
                createRow(pk4, "2023-11-13 10:50:00.123456", Delete, "data4")
        ), TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS);

        underTest.processBatch(spark, sourceReference, input);

        thenStructuredCuratedAndOperationalDataStoreContainForPK("data1", pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK("data2", pk2, testQueryConnection);

        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk4, testQueryConnection);
    }

    @Test
    public void shouldWriteNullsToViolationsForNonNullableColumns() throws Exception {
        Dataset<Row> input = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA);
        underTest.processBatch(spark, sourceReference, input);

        thenStructuredCuratedAndOperationalDataStoreContainForPK("data1", pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreContainForPK("data3", pk3, testQueryConnection);

        thenStructuredViolationsContainsForPK("data2", pk2);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
    }

    @Test
    public void shouldWriteToViolationsForDfWithExtraColumn() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("extra-column", lit(1));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    public void shouldWriteToViolationsForDfWithMissingColumn() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).drop("data");
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    public void shouldWriteToViolationsForDfWhenTypeGoesFromStringToInt() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    public void shouldWriteToViolationsForDfWhenTypeGoesFromIntToString() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    public void shouldWriteToViolationsForDfWhenTypeGoesFromIntToLong() throws Exception {
        Dataset<Row> dfWithMisMatchingSchema = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA).withColumn("data", lit(1L));
        underTest.processBatch(spark, sourceReference, dfWithMisMatchingSchema);

        thenStructuredViolationsContainsPK(pk1);
        thenStructuredViolationsContainsPK(pk2);
        thenStructuredViolationsContainsPK(pk3);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk1, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk2, testQueryConnection);
        thenStructuredCuratedAndOperationalDataStoreDoNotContainPK(pk3, testQueryConnection);
    }

    @Test
    public void shouldWriteToViolationsWhenSchemaChangesFromWhatIsAlreadyInViolations() throws Exception {
        // The 1st bad dataframe will be written to violations with one schema
        Dataset<Row> dfNullNonNullableCols = spark.createDataFrame(Arrays.asList(
                createRow(pk1, "2023-11-13 10:50:00.123456", Insert, "data1"),
                createRow(pk2, null, Insert, "data2"),
                createRow(pk3, "2023-11-13 10:50:00.123456", Insert, "data3")
        ), TEST_DATA_SCHEMA);
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

    private void givenS3BatchProcessorDependenciesAreInjected() {
        DataStorageService storageService = new DataStorageService(arguments);
        S3DataProvider dataProvider = new S3DataProvider(arguments);
        TableDiscoveryService tableDiscoveryService = new TableDiscoveryService(arguments, configService);
        ViolationService violationService = new ViolationService(arguments, storageService, dataProvider, tableDiscoveryService);
        ValidationService validationService = new ValidationService(violationService);
        StructuredZoneLoad structuredZoneLoad = new StructuredZoneLoad(arguments, storageService, violationService);
        CuratedZoneLoad curatedZoneLoad = new CuratedZoneLoad(arguments, storageService, violationService);
        OperationalDataStoreTransformation operationalDataStoreTransformation = new OperationalDataStoreTransformation();
        ConnectionPoolProvider connectionPoolProvider = new ConnectionPoolProvider();
        OperationalDataStoreRepositoryProvider operationalDataStoreRepositoryProvider = new OperationalDataStoreRepositoryProvider(arguments);
        OperationalDataStoreDataAccess operationalDataStoreDataAccess =
                new OperationalDataStoreDataAccess(connectionDetailsService, connectionPoolProvider, operationalDataStoreRepositoryProvider);
        OperationalDataStoreService operationalDataStoreService =
                new OperationalDataStoreServiceImpl(arguments, operationalDataStoreTransformation, operationalDataStoreDataAccess);
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
        when(sourceReference.getSource()).thenReturn(inputSchemaName);
        when(sourceReference.getTable()).thenReturn(inputTableName);
        when(sourceReference.getFullyQualifiedTableName()).thenReturn(format("%s.%s", inputSchemaName, inputTableName));
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
    }
}