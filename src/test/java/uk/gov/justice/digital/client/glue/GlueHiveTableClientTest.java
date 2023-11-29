package uk.gov.justice.digital.client.glue;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.client.glue.GlueHiveTableClient.*;
import static uk.gov.justice.digital.test.Fixtures.JSON_DATA_SCHEMA;

@ExtendWith(MockitoExtension.class)
class GlueHiveTableClientTest {

    @Mock
    private GlueClientProvider mockClientProvider;

    @Mock
    private AWSGlue mockGlueClient;

    @Captor
    ArgumentCaptor<CreateTableRequest> createTableRequestCaptor;

    @Captor
    ArgumentCaptor<DeleteTableRequest> deleteTableRequestCaptor;

    private static final String TEST_DATABASE = "test-database";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_PATH = "/some/data/path";

    private GlueHiveTableClient underTest;

    @BeforeEach
    public void setup() {
        when(mockClientProvider.getClient()).thenReturn(mockGlueClient);
        underTest = new GlueHiveTableClient(mockClientProvider);
    }

    @Test
    void shouldCreateParquetTable() {
        underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        StorageDescriptor storageDescriptor = createTableRequestCaptor.getValue().getTableInput().getStorageDescriptor();
        assertThat(storageDescriptor.getLocation(), equalTo(TEST_PATH));
        assertThat(storageDescriptor.getInputFormat(), equalTo(MAPRED_PARQUET_INPUT_FORMAT));
    }

    @Test
    public void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingParquetTable() {
        when(mockGlueClient.createTable(any())).thenThrow(new AWSGlueException("failed to create table"));

        assertThrows(
                AWSGlueException.class,
                () -> underTest.createParquetTable(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA)
        );
    }

    @Test
    void shouldCreateTableWithSymlink() {
        underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA);

        verify(mockGlueClient, times(1)).createTable(createTableRequestCaptor.capture());

        StorageDescriptor storageDescriptor = createTableRequestCaptor.getValue().getTableInput().getStorageDescriptor();
        assertThat(storageDescriptor.getLocation(), equalTo(TEST_PATH + "/_symlink_format_manifest"));
        assertThat(storageDescriptor.getInputFormat(), equalTo(SYMLINK_INPUT_FORMAT));
    }

    @Test
    public void shouldThrowAnExceptionIfAnErrorOccursWhenCreatingSymlinkTable() {
        when(mockGlueClient.createTable(any())).thenThrow(new AWSGlueException("failed to create table"));

        assertThrows(
                AWSGlueException.class,
                () -> underTest.createTableWithSymlink(TEST_DATABASE, TEST_TABLE, TEST_PATH, JSON_DATA_SCHEMA)
        );
    }

    @Test
    void shouldDeleteTable() {
        underTest.deleteTable(TEST_DATABASE, TEST_TABLE);

        verify(mockGlueClient, times(1)).deleteTable(deleteTableRequestCaptor.capture());

        DeleteTableRequest deleteTableRequest = deleteTableRequestCaptor.getValue();
        assertThat(deleteTableRequest.getDatabaseName(), equalTo(TEST_DATABASE));
        assertThat(deleteTableRequest.getName(), equalTo(TEST_TABLE));
    }

    @Test
    public void shouldNotFailWhenAnAttemptIsMadeToDeleteTableThatDoesNotExist() {
        when(mockGlueClient.deleteTable(any())).thenThrow(new EntityNotFoundException("failed to delete non-existent table"));

        assertDoesNotThrow(() -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }

    @Test
    public void shouldThrowAnExceptionIfAnyOtherErrorOccursWhenDeletingTable() {
        when(mockGlueClient.deleteTable(any())).thenThrow(new AWSGlueException("failed to delete table"));

        assertThrows(AWSGlueException.class, () -> underTest.deleteTable(TEST_DATABASE, TEST_TABLE));
    }
}