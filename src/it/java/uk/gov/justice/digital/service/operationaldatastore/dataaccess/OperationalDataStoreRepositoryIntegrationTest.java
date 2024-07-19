package uk.gov.justice.digital.service.operationaldatastore.dataaccess;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.DataHubOperationalDataStoreManagedTable;
import uk.gov.justice.digital.test.InMemoryOperationalDataStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenDatastoreCredentials;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenSchemaExists;
import static uk.gov.justice.digital.test.SharedTestFunctions.givenTablesToWriteContains;

@ExtendWith(MockitoExtension.class)
public class OperationalDataStoreRepositoryIntegrationTest extends BaseSparkTest {
    private static final InMemoryOperationalDataStore operationalDataStore = new InMemoryOperationalDataStore();
    private static Connection testQueryConnection;
    private static DataSource dataSource;

    @Mock
    private JobArguments jobArguments;
    @Mock
    private OperationalDataStoreConnectionDetailsService connectionDetailsService;

    private OperationalDataStoreRepository underTest;

    @BeforeAll
    static void beforeAll() throws Exception {
        operationalDataStore.start();
        testQueryConnection = operationalDataStore.getJdbcConnection();
        dataSource = operationalDataStore.getConnectionPool();
    }

    @AfterAll
    static void afterAll() throws Exception {
        testQueryConnection.close();
        operationalDataStore.stop();
    }

    @BeforeEach
    void setUp() {
        givenDatastoreCredentials(connectionDetailsService, operationalDataStore);
        underTest = new OperationalDataStoreRepository(jobArguments, connectionDetailsService, sparkSessionProvider);
    }

    @Test
    void shouldRetrieveAllEntriesFromTable() throws Exception {
        String schemaName = "config_for_repository_it";
        String tableName = "operationaldatastorerepositoryintegrationtest";

        givenSchemaExists(schemaName, testQueryConnection);
        givenTablesToWriteContains(schemaName, tableName, "my_source", "my_table_name", testQueryConnection);

        when(jobArguments.getOperationalDataStoreTablesToWriteTableName()).thenReturn(schemaName + "." + tableName);

        Set<DataHubOperationalDataStoreManagedTable> result = underTest.getDataHubOperationalDataStoreManagedTables();
        Set<DataHubOperationalDataStoreManagedTable> expected = new HashSet<>(Arrays.asList(
                new DataHubOperationalDataStoreManagedTable("my_source", "my_table_name")
        ));
        assertEquals(expected, result);
    }
}
