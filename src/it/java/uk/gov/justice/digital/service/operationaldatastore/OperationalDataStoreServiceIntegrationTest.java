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
import org.testcontainers.containers.PostgreSQLContainer;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreConnectionDetails;
import uk.gov.justice.digital.datahub.model.OperationalDataStoreCredentials;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.spark.sql.functions.col;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

/**
 * This test requires a Docker environment, such as <a href="https://github.com/abiosoft/colima">Colima</a>.
 * To run with Colima
 * export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=${HOME}/.colima/docker.sock
 * export DOCKER_HOST="unix:///${HOME}/.colima/docker.sock"
 */
@ExtendWith(MockitoExtension.class)
public class OperationalDataStoreServiceIntegrationTest extends BaseSparkTest {

    private static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            "postgres:16-alpine"
    );

    private static final StructType schema = new StructType(new StructField[]{
            new StructField("PK", DataTypes.StringType, true, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()),
            new StructField("DATA", DataTypes.StringType, true, Metadata.empty())
    });

    private static final String tableName = "public.my_test_table";

    @Mock
    private OperationalDataStoreConnectionDetailsService mockConnectionDetailsService;
    @Mock
    private SourceReference sourceReference;

    private OperationalDataStoreService underTest;

    @BeforeAll
    static void beforeAll() {
        postgres.start();
    }

    @AfterAll
    static void afterAll() {
        postgres.stop();
    }

    @BeforeEach
    void setUp() {
        // Use the TestContainers Postgres connection details
        OperationalDataStoreCredentials credentials = new OperationalDataStoreCredentials();
        credentials.setUsername(postgres.getUsername());
        credentials.setPassword(postgres.getPassword());

        when(mockConnectionDetailsService.getConnectionDetails()).thenReturn(
                new OperationalDataStoreConnectionDetails(postgres.getJdbcUrl(), credentials)
        );

        when(sourceReference.getSchema()).thenReturn(schema);

        underTest = new OperationalDataStoreService(
                new OperationalDataStoreDataTransformation(),
                new OperationalDataStoreDataAccess(mockConnectionDetailsService)
        );
    }

    @Test
    public void shouldInsertData() {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other data")
        ), schema);

        underTest.storeBatchData(df, sourceReference);

        Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", postgres.getUsername());
        jdbcProperties.put("password", postgres.getPassword());

        Dataset<Row> result = spark.read().jdbc(postgres.getJdbcUrl(), tableName, jdbcProperties);

        assertEquals(2, result.count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("pk").contains("pk2")).count());
        assertEquals(1, result.where(col("data").contains("some data")).count());
        assertEquals(1, result.where(col("data").contains("some other data")).count());
    }

    @Test
    public void shouldOverwriteExistingData() {
        Dataset<Row> df1 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other data")
        ), schema);

        Dataset<Row> df2 = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some new data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other new data")
        ), schema);

        underTest.storeBatchData(df1, sourceReference);
        underTest.storeBatchData(df2, sourceReference);

        Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", postgres.getUsername());
        jdbcProperties.put("password", postgres.getPassword());

        Dataset<Row> result = spark.read().jdbc(postgres.getJdbcUrl(), tableName, jdbcProperties);

        assertEquals(2, result.count());

        assertEquals(1, result.where(col("pk").contains("pk1")).count());
        assertEquals(1, result.where(col("pk").contains("pk2")).count());
        assertEquals(1, result.where(col("data").contains("some new data")).count());
        assertEquals(1, result.where(col("data").contains("some other new data")).count());
        assertEquals(0, result.where(col("data").contains("some data")).count());
        assertEquals(0, result.where(col("data").contains("some other data")).count());
    }

    @Test
    public void shouldCreateTableWithLowercaseColumnsWithoutOpAndTimestamp() {
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("pk1", "2023-11-13 10:49:28.123458", "I", "some data"),
                RowFactory.create("pk2", "2023-11-13 10:49:28.123458", "I", "some other data")
        ), schema);

        underTest.storeBatchData(df, sourceReference);

        Properties jdbcProperties = new Properties();
        jdbcProperties.put("user", postgres.getUsername());
        jdbcProperties.put("password", postgres.getPassword());

        Dataset<Row> result = spark.read().jdbc(postgres.getJdbcUrl(), tableName, jdbcProperties);

        assertThat(result.columns(), arrayContainingInAnyOrder("pk", "data"));
        assertThat(result.columns(), not(arrayContainingInAnyOrder(OPERATION, OPERATION.toLowerCase(), TIMESTAMP)));
    }
}
