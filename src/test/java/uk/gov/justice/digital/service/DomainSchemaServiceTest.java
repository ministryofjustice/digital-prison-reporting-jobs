package uk.gov.justice.digital.service;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import com.amazonaws.services.glue.AWSGlueClient;
import com.amazonaws.services.glue.model.*;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.domain.model.TableInfo;
import uk.gov.justice.digital.exception.DomainSchemaException;

@MicronautTest
public class DomainSchemaServiceTest extends BaseSparkTest {

    @Mock
    public AWSGlueClient mockGlueClient = mock(AWSGlueClient.class);

    @Test
    public void shouldCreateDomainSchemaService() {
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        assertNotNull(service);
    }

    @Test
    public void shouldReturnTrueWhenADatabaseExists() {
        final GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("name"));
        when(mockGlueClient.getDatabase(any())).thenReturn(result);

        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        assertTrue(service.databaseExists("name"));
        verify(mockGlueClient, times(1)).getDatabase(any());
    }

    @Test
    public void shouldReturnFalseWhenADatabaseDoesntExist() {
        final GetDatabaseResult result = new GetDatabaseResult().withDatabase(null);
        when(mockGlueClient.getDatabase(any())).thenReturn(result);

        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        assertFalse(service.databaseExists("name"));
        verify(mockGlueClient, times(1)).getDatabase(any());
    }

    @Test
    public void shouldReturnTrueIfTableExists() {
        final GetTableResult result = new GetTableResult();
        when(mockGlueClient.getTable(any())).thenReturn(result);
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        assertTrue(service.tableExists("name", "table"));
        verify(mockGlueClient, times(1)).getTable(any());
    }

    @Test
    public void shouldReturnFalseIfTableDoesntExist() {
        when(mockGlueClient.getTable(any())).thenThrow(new EntityNotFoundException("message"));
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        assertFalse(service.tableExists("name", "table"));
        verify(mockGlueClient, times(1)).getTable(any());
    }

    @Test
    public void shouldCreateATableWhenDataFrameProvided() {
        when(mockGlueClient.createTable(any())).thenReturn(new CreateTableResult());
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        final Dataset<Row> df = spark.emptyDataFrame();
        service.createTable("database", "table", "path", df);
        verify(mockGlueClient, times(1)).createTable(any());
    }

    @Test
    public void shouldUpdateATableWhenPreviouslyExistsAndDataFrameProvided() {
        when(mockGlueClient.createTable(any())).thenReturn(new CreateTableResult());
        when(mockGlueClient.deleteTable(any())).thenReturn(new DeleteTableResult());
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        final Dataset<Row> df = spark.emptyDataFrame();
        service.updateTable("database", "table", "path", df);
        verify(mockGlueClient, times(1)).createTable(any());
        verify(mockGlueClient, times(1)).deleteTable(any());
    }

    @Test
    public void shouldDeleteATableWhenDataFrameProvided() {
        when(mockGlueClient.deleteTable(any())).thenReturn(new DeleteTableResult());
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        final Dataset<Row> df = spark.emptyDataFrame();
        service.deleteTable("database", "table");
        verify(mockGlueClient, times(1)).deleteTable(any());
    }

    @Test
    public void shouldCreateATableWhenCreateIsCalled() throws DomainSchemaException {

        final GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("database"));
        when(mockGlueClient.getDatabase(any())).thenReturn(result);
        when(mockGlueClient.getTable(any())).thenThrow(new EntityNotFoundException(""));
        when(mockGlueClient.createTable(any())).thenReturn(new CreateTableResult());
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        final Dataset<Row> df = spark.emptyDataFrame();
        TableInfo info = TableInfo.create("prefix", "database", "schema", "table");
        service.create(info, "table", df);
        verify(mockGlueClient, times(1)).createTable(any());
        verify(mockGlueClient, times(0)).deleteTable(any());

    }

    @Test
    public void shouldReplaceATableWhenReplaceIsCalled() throws DomainSchemaException {

        final GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("database"));
        when(mockGlueClient.getDatabase(any())).thenReturn(result);
        when(mockGlueClient.getTable(any())).thenReturn(new GetTableResult());
        when(mockGlueClient.createTable(any())).thenReturn(new CreateTableResult());
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        final Dataset<Row> df = spark.emptyDataFrame();
        TableInfo info = TableInfo.create("prefix", "database", "schema", "table");
        service.replace(info, "table", df);
        verify(mockGlueClient, times(1)).createTable(any());
        verify(mockGlueClient, times(1)).deleteTable(any());

    }


    @Test
    public void shouldDropATableWhenDropIsCalled() throws DomainSchemaException {

        final GetDatabaseResult result = new GetDatabaseResult().withDatabase(new Database().withName("database"));
        when(mockGlueClient.getDatabase(any())).thenReturn(result);
        when(mockGlueClient.getTable(any())).thenReturn(new GetTableResult());
        when(mockGlueClient.createTable(any())).thenReturn(new CreateTableResult());
        final DomainSchemaService service = new DomainSchemaService(mockGlueClient);
        final Dataset<Row> df = spark.emptyDataFrame();
        TableInfo info = TableInfo.create("prefix", "database", "schema", "table");
        service.drop(info);
        verify(mockGlueClient, times(0)).createTable(any());
        verify(mockGlueClient, times(1)).deleteTable(any());

    }
}
