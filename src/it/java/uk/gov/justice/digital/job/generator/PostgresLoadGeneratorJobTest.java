package uk.gov.justice.digital.job.generator;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.secretsmanager.SecretsManagerClient;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.generator.PostgresSecrets;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostgresLoadGeneratorJobTest extends SparkTestBase {

    private static final InMemoryOperationalDataStore dataStore = new InMemoryOperationalDataStore();

    private static final String TEST_SECRET_ID = "test-secret-id";
    private static final String TEST_TABLE = "local_test_table";

    private static Connection connection;

    @Mock
    private JobArguments mockJobArguments;
    @Mock
    private SecretsManagerClient mockSecretsManagerClient;
    @Mock
    private PostgresSecrets mockPostgresSecrets;

    private PostgresLoadGeneratorJob underTest;

    @BeforeAll
    static void beforeAll() throws Exception {
        dataStore.start();
        connection = dataStore.getJdbcConnection();
        createTable();
    }

    @AfterAll
    static void afterAll() throws SQLException {
        connection.close();
        dataStore.stop();
    }

    @BeforeEach
    void setup() throws SQLException {
        truncateTable();
        reset(mockSecretsManagerClient);

        when(mockJobArguments.getSecretId()).thenReturn(TEST_SECRET_ID);
        when(mockJobArguments.getTestDataTableName()).thenReturn(TEST_TABLE);

        when(mockPostgresSecrets.getJdbcUrl()).thenReturn(dataStore.getJdbcUrl());
        when(mockPostgresSecrets.getUsername()).thenReturn(dataStore.getUsername());
        when(mockPostgresSecrets.getPassword()).thenReturn(dataStore.getPassword());

        when(mockSecretsManagerClient.getSecret(TEST_SECRET_ID, PostgresSecrets.class)).thenReturn(mockPostgresSecrets);

        underTest = new PostgresLoadGeneratorJob(mockJobArguments, mockSecretsManagerClient);
    }

    @Test
    void shouldInsertSingleRecord() throws SQLException {
        when(mockJobArguments.getTestDataParallelism()).thenReturn(1);
        when(mockJobArguments.getTestDataBatchSize()).thenReturn(1);
        when(mockJobArguments.getRunDurationMillis()).thenReturn(20L);
        when(mockJobArguments.getTestDataInterBatchDelayMillis()).thenReturn(20L);

        underTest.run();

        assertEquals(1, countRecords());
    }

    @Test
    void shouldInsertMultipleRecordsWhenGivenIncreasedBatchSize() throws SQLException {
        when(mockJobArguments.getTestDataParallelism()).thenReturn(1);
        when(mockJobArguments.getTestDataBatchSize()).thenReturn(2);
        when(mockJobArguments.getRunDurationMillis()).thenReturn(20L);
        when(mockJobArguments.getTestDataInterBatchDelayMillis()).thenReturn(20L);

        underTest.run();

        assertEquals(2, countRecords());
    }

    @Test
    void shouldInsertMultipleRecordsGivenIncreasedParallelism() throws SQLException {
        when(mockJobArguments.getTestDataParallelism()).thenReturn(2);
        when(mockJobArguments.getTestDataBatchSize()).thenReturn(1);
        when(mockJobArguments.getRunDurationMillis()).thenReturn(20L);
        when(mockJobArguments.getTestDataInterBatchDelayMillis()).thenReturn(20L);

        underTest.run();

        assertEquals(2, countRecords());
    }

    @Test
    void shouldInsertMultipleRecordsGivenIncreasedParallelismAndBatchSize() throws SQLException {
        when(mockJobArguments.getTestDataParallelism()).thenReturn(2);
        when(mockJobArguments.getTestDataBatchSize()).thenReturn(2);
        when(mockJobArguments.getRunDurationMillis()).thenReturn(20L);
        when(mockJobArguments.getTestDataInterBatchDelayMillis()).thenReturn(20L);

        underTest.run();

        assertEquals(4, countRecords());
    }

    @Test
    void shouldInsertMultipleRecordsGivenIncreasedRunDuration() throws SQLException {
        when(mockJobArguments.getTestDataParallelism()).thenReturn(2);
        when(mockJobArguments.getTestDataBatchSize()).thenReturn(2);
        when(mockJobArguments.getRunDurationMillis()).thenReturn(200L);
        when(mockJobArguments.getTestDataInterBatchDelayMillis()).thenReturn(20L);

        underTest.run();

        assertThat(countRecords(), greaterThan(4L));
    }

    private static void createTable() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute("CREATE SEQUENCE id_seq");
        statement.execute(format("CREATE TABLE IF NOT EXISTS %s (id INTEGER DEFAULT NEXTVAL('id_seq'), data VARCHAR)", TEST_TABLE));
    }

    private static void truncateTable() throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(format("TRUNCATE TABLE %s", TEST_TABLE));
    }

    private static long countRecords() throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(format("SELECT COUNT(*) FROM %s", TEST_TABLE));
        result.next();
        return result.getLong(1);
    }

}
