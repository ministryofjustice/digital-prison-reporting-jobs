package uk.gov.justice.digital.domain;

import lombok.val;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.service.DomainSchemaService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DomainExecutorRefreshTest extends DomainExecutorTest {

    @Test
    public void shouldRunWithFullUpdateIfTableIsInDomain() throws Exception {
        val domain = getDomain("/sample/domain/sample-domain-execution.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        // save a source
        helpers.persistDataset(
                new TableIdentifier(sourcePath(), hiveDatabaseName, "source", "table"),
                helpers.getOffenders(tmp)
        );

        val domainTableName = "prisoner";
        // Insert first
        executor.doFullDomainRefresh(domain, domainTableName, "insert");
        // then update
        executor.doFullDomainRefresh(domain, domainTableName, "update");
        verify(testSchemaService, times(1)).create(any(), any(), any());
        verify(testSchemaService, times(1)).replace(any(), any(), any());

        // there should be a target table
        val info = new TableIdentifier(targetPath(), hiveDatabaseName, domain.getName(), domainTableName);
        assertTrue(storage.exists(spark, info));

        // it should have all the offenders in it
        assertTrue(areEqual(helpers.getOffenders(tmp), storage.get(spark, info)));
    }

    @Test
    public void shouldRunWithFullUpdateIfMultipleTablesAreInDomain() throws Exception {
        val domain1 = getDomain("/sample/domain/sample-domain-execution-insert.json");
        val domain2 = getDomain("/sample/domain/sample-domain-execution-join.json");

        // save a source
        helpers.persistDataset(
                new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "offenders"),
                helpers.getOffenders(tmp)
        );
        helpers.persistDataset(
                new TableIdentifier(sourcePath(), hiveDatabaseName, "nomis", "offender_bookings"),
                helpers.getOffenderBookings(tmp)
        );

        // do Full Materialize of source to target
        val domainTableName = "prisoner";
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        executor.doFullDomainRefresh(domain1, domainTableName, "insert");

        verify(testSchemaService, times(1)).create(any(), any(), any());

        // there should be a target table
        val info = new TableIdentifier(targetPath(), hiveDatabaseName, "example", "prisoner");
        assertTrue(storage.exists(spark, info));
        // it should have all the joined records in it
        assertEquals(1, storage.get(spark, info).count());

        // now the reverse
        executor.doFullDomainRefresh(domain2, domainTableName, "update");
        verify(testSchemaService, times(1)).replace(any(), any(), any());

        // there should be a target table
        assertTrue(storage.exists(spark, info));
        // it should have all the joined records in it
        assertEquals(1, storage.get(spark, info).count());
    }

    @Test
    public void shouldRunWithNoChangesIfTableIsNotInDomain() throws Exception {

        val domain = getDomain("/sample/domain/sample-domain-execution-bad-source-table.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        helpers.persistDataset(
                new TableIdentifier(sourcePath(), hiveDatabaseName, "source", "table"),
                helpers.getOffenders(tmp)
        );

        assertThrows(
                DomainExecutorException.class,
                () -> executor.doFullDomainRefresh(domain, "prisoner", "insert")
        );

        // there shouldn't be a target table
        TableIdentifier info = new TableIdentifier(targetPath(), hiveDatabaseName, "example", "prisoner");
        assertFalse(storage.exists(spark, info));
    }

}