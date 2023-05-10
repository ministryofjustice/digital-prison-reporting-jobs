package uk.gov.justice.digital.service;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import uk.gov.justice.digital.client.glue.GlueClientProvider;
import uk.gov.justice.digital.domain.model.TableInfo;
import uk.gov.justice.digital.exception.DomainSchemaException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
// Allow common mocks to be defined before each test.
@MockitoSettings(strictness = Strictness.LENIENT)
public class DomainSchemaServiceTest {

    @Mock
    private AWSGlue mockClient;
    @Mock
    private GlueClientProvider mockClientProvider;
    @Mock
    private Dataset<Row> mockDataframe;
    @Mock
    private StructType mockSchema;

    private DomainSchemaService underTest;

    @BeforeEach
    public void setupMocks() {
        when(mockSchema.fields()).thenReturn(new StructField[0]);
        when(mockDataframe.schema()).thenReturn(mockSchema);
        when(mockClientProvider.getClient()).thenReturn(mockClient);
        underTest = new DomainSchemaService(mockClientProvider);
    }

    @Test
    public void shouldReturnTrueWhenADatabaseExists() {
        GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("name"));
        when(mockClient.getDatabase(any())).thenReturn(result);

        assertTrue(underTest.databaseExists("name"));
        verify(mockClient, times(1)).getDatabase(any());
    }

    @Test
    public void shouldReturnFalseWhenADatabaseDoesntExist() {
        GetDatabaseResult result = new GetDatabaseResult().withDatabase(null);
        when(mockClient.getDatabase(any())).thenReturn(result);

        assertFalse(underTest.databaseExists("name"));
        verify(mockClient, times(1)).getDatabase(any());
    }

    @Test
    public void shouldReturnTrueIfTableExists() {
        GetTableResult result = new GetTableResult();
        when(mockClient.getTable(any())).thenReturn(result);
        assertTrue(underTest.tableExists("name", "table"));
        verify(mockClient, times(1)).getTable(any());
    }

    @Test
    public void shouldReturnFalseIfTableDoesntExist() {
        when(mockClient.getTable(any())).thenThrow(new EntityNotFoundException("message"));
        assertFalse(underTest.tableExists("name", "table"));
        verify(mockClient, times(1)).getTable(any());
    }

    @Test
    public void shouldCreateATableWhenDataFrameProvided() {
        when(mockClient.createTable(any())).thenReturn(new CreateTableResult());
        underTest.createTable("database", "table", "path", mockDataframe);
        verify(mockClient, times(1)).createTable(any());
    }

    @Test
    public void shouldUpdateATableWhenPreviouslyExistsAndDataFrameProvided() {
        when(mockClient.createTable(any())).thenReturn(new CreateTableResult());
        when(mockClient.deleteTable(any())).thenReturn(new DeleteTableResult());
        underTest.updateTable("database", "table", "path", mockDataframe);
        verify(mockClient, times(1)).createTable(any());
        verify(mockClient, times(1)).deleteTable(any());
    }

    @Test
    public void shouldDeleteATableWhenDataFrameProvided() {
        when(mockClient.deleteTable(any())).thenReturn(new DeleteTableResult());
        underTest.deleteTable("database", "table");
        verify(mockClient, times(1)).deleteTable(any());
    }

    @Test
    public void shouldCreateATableWhenCreateIsCalled() throws DomainSchemaException {
        GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("database"));
        when(mockClient.getDatabase(any())).thenReturn(result);
        when(mockClient.getTable(any())).thenThrow(new EntityNotFoundException(""));
        when(mockClient.createTable(any())).thenReturn(new CreateTableResult());
        TableInfo info = new TableInfo("prefix", "database", "schema", "table");
        underTest.create(info, "table", mockDataframe);
        verify(mockClient, times(1)).createTable(any());
        verify(mockClient, times(0)).deleteTable(any());
    }

    @Test
    public void shouldReplaceATableWhenReplaceIsCalled() throws DomainSchemaException {
        GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("database"));
        when(mockClient.getDatabase(any())).thenReturn(result);
        when(mockClient.getTable(any())).thenReturn(new GetTableResult());
        when(mockClient.createTable(any())).thenReturn(new CreateTableResult());
        TableInfo info = new TableInfo("prefix", "database", "schema", "table");
        underTest.replace(info, "table", mockDataframe);
        verify(mockClient, times(1)).createTable(any());
        verify(mockClient, times(1)).deleteTable(any());
    }


    @Test
    public void shouldDropATableWhenDropIsCalled() throws DomainSchemaException {
        GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("database"));
        when(mockClient.getDatabase(any())).thenReturn(result);
        when(mockClient.getTable(any())).thenReturn(new GetTableResult());
        TableInfo info = new TableInfo("prefix", "database", "schema", "table");
        underTest.drop(info);
        verify(mockClient, times(0)).createTable(any());
        verify(mockClient, times(1)).deleteTable(any());
    }

}
