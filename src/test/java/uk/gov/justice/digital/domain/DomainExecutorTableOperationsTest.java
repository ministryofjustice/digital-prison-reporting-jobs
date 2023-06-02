package uk.gov.justice.digital.domain;

import lombok.val;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.exception.DomainSchemaException;
import uk.gov.justice.digital.service.DomainSchemaService;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DomainExecutorTableOperationsTest extends DomainExecutorTest {

    @Test
    public void shouldGetAllSourcesForTable() throws Exception {
        val domainDefinition = getDomain("/sample/domain/incident_domain.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        helpers.persistDataset(
                new TableIdentifier(SAMPLE_EVENTS_PATH, hiveDatabaseName, "nomis", "offender_bookings"),
                helpers.getOffenderBookings(tmp)
        );

        helpers.persistDataset(
                new TableIdentifier(SAMPLE_EVENTS_PATH, hiveDatabaseName, "nomis", "offenders"),
                helpers.getOffenders(tmp)
        );

        for (val table : domainDefinition.getTables()) {
            for (val source : table.getTransform().getSources()) {
                val dataFrame = executor.getAllSourcesForTable(SAMPLE_EVENTS_PATH, source, null);
                assertNotNull(dataFrame);
            }
        }
    }

    @Test
    public void shouldSaveTable() throws Exception {
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        val domainDefinition = getDomain("/sample/domain/incident_domain.json");

        for (val table : domainDefinition.getTables()) {
            val df = executor.apply(table, getOffenderRefs());
            val targetInfo = new TableIdentifier(
                    domainTargetPath(),
                    hiveDatabaseName,
                    domainDefinition.getName(),
                    table.getName()
            );
            executor.saveTable(targetInfo, df, "insert");
        }

        verify(testSchemaService, atLeastOnce()).create(any(), any(), any());
    }

    @Test
    public void shouldOverwriteWhenTableExists()
            throws DomainExecutorException, DomainSchemaException {
        TableIdentifier tbl = new TableIdentifier("s3://somepath", "test",
                "incident", "demographics" );
        doReturn(true).when(testStorage).exists(spark, tbl);
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), testStorage, testSchemaService);
        executor.insertTable(tbl, mockedDataSet);
        verify(testSchemaService, times(1)).create(any(), any(), any());
    }

    @Test
    public void shouldCreateSchemaAndTable() throws DomainExecutorException, DomainSchemaException {
        TableIdentifier tbl = new TableIdentifier("s3://somepath", "test",
                "incident", "demographics" );
        doReturn(false).when(testStorage).exists(spark, tbl);
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), testStorage, testSchemaService);
        executor.insertTable(tbl, mockedDataSet);
        verify(testSchemaService, times(1)).create(any(), any(), any());
    }

    @Test
    public void shouldInsertTableIfTableDoesNotExists() throws DomainExecutorException, DomainSchemaException {
        TableIdentifier tbl = new TableIdentifier("s3://somepath", "test",
                "incident", "demographics" );
        doReturn(false).when(testStorage).exists(spark, tbl);
        val testSchemaService = mock(DomainSchemaService.class);
        when(testSchemaService.tableExists("test", "incident_demographics")).thenReturn(false);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), testStorage, testSchemaService);
        executor.insertTable(tbl, mockedDataSet);
        verify(testSchemaService, times(1)).create(any(), any(), any());
    }

    @Test
    public void shouldUpdateTableIfTableExists() throws DomainExecutorException, DomainSchemaException {
        TableIdentifier tbl = new TableIdentifier("s3://somepath", "test",
                "incident", "demographics" );
        doReturn(true).when(testStorage).exists(spark, tbl);
        val testSchemaService = mock(DomainSchemaService.class);
        when(testSchemaService.tableExists("test", "incident_demographics")).thenReturn(true);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), testStorage, testSchemaService);
        executor.updateTable(tbl, mockedDataSet);
        verify(testSchemaService, times(1)).replace(any(), any(), any());
    }

    @Test
    public void shouldSyncTableIfTableExists() throws DomainExecutorException, DataStorageException {
        TableIdentifier tbl = new TableIdentifier("s3://somepath", "test",
                "incident", "demographics" );
        doReturn(true).when(testStorage).exists(spark, tbl);
        val testSchemaService = mock(DomainSchemaService.class);
        when(testSchemaService.tableExists("test", "incident_demographics")).thenReturn(true);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), testStorage, testSchemaService);
        executor.syncTable(tbl, mockedDataSet);
        verify(testStorage, times(1)).resync(any(), any());
    }

    @Test
    public void shouldDeleteTableIfTableExists() throws DomainExecutorException, DomainSchemaException {
        TableIdentifier tbl = new TableIdentifier("s3://somepath", "test",
                "incident", "demographics" );
        doReturn(true).when(testStorage).exists(spark, tbl);
        val testSchemaService = mock(DomainSchemaService.class);
        when(testSchemaService.tableExists("test", "incident_demographics")).thenReturn(true);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), testStorage, testSchemaService);
        executor.deleteTable(tbl);
        verify(testSchemaService, times(1)).drop(any());
    }

    @Test
    public void shouldDeleteTable() throws Exception {
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        helpers.persistDataset(
                new TableIdentifier(SAMPLE_EVENTS_PATH, hiveDatabaseName, "nomis", "offender_bookings"),
                helpers.getOffenderBookings(tmp)
        );

        helpers.persistDataset(
                new TableIdentifier(SAMPLE_EVENTS_PATH, hiveDatabaseName, "nomis", "offenders"),
                helpers.getOffenders(tmp)
        );

        val domainDefinition = getDomain("/sample/domain/incident_domain.json");
        executor.doFullDomainRefresh(
                domainDefinition,
                "demographics",
                "insert"
        );

        for (val table : domainDefinition.getTables()) {
            executor.deleteTable(new TableIdentifier(
                    domainTargetPath(),
                    hiveDatabaseName,
                    domainDefinition.getName(),
                    table.getName()
            ));
        }

        verify(testSchemaService, times(1)).drop(any());
    }

}
