package uk.gov.justice.digital.domain;

import lombok.val;
import org.junit.jupiter.api.Test;
import uk.gov.justice.digital.domain.model.TableDefinition;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.service.DomainSchemaService;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class DomainExecutorApplyOperationsTest extends DomainExecutorTest {

    @Test
    public void shouldTestApplyDomain() throws Exception {
        val domainDefinition = getDomain("/sample/domain/incident_domain.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        for (val table : domainDefinition.getTables()) {
            val dataframe = executor.apply(table, getOffenderRefs());
            assertEquals(dataframe.schema(), helpers.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void shouldApplyTransform() throws Exception {
        val domainDefinition = getDomain("/sample/domain/incident_domain.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        for (val table : domainDefinition.getTables()) {
            val transformedDataFrame = executor.applyTransform(getOffenderRefs(), table.getTransform());
            assertEquals(transformedDataFrame.schema(), helpers.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void shouldApplyViolationsIfEmpty() throws Exception {
        val domainDefinition = getDomain("/sample/domain/incident_domain.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        for (val table : domainDefinition.getTables()) {
            val transformedDataFrame = executor.applyTransform(getOffenderRefs(), table.getTransform());
            val postViolationsDataFrame = executor.applyViolations(transformedDataFrame, table.getViolations());
            assertEquals(postViolationsDataFrame.schema(), helpers.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void shouldApplyViolationsIfNonEmpty() throws Exception {
        val domainDefinition = getDomain("/sample/domain/domain-violations-check.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, targetPath(), storage, testSchemaService);

        val refs = Collections.singletonMap("source.table", helpers.getOffenders(tmp));

        for (val table : domainDefinition.getTables()) {
            val transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            val postViolationsDataFrame = executor.applyViolations(transformedDataFrame, table.getViolations());
            assertEquals(postViolationsDataFrame.schema(), helpers.createViolationsDomainDataframe().schema());
        }
    }

    @Test
    public void shouldApplyMappings() throws Exception {
        val domainDefinition = getDomain("/sample/domain/incident_domain.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        for (val table : domainDefinition.getTables()) {
            val transformedDataFrame = executor.applyTransform(getOffenderRefs(), table.getTransform());
            val postViolationsDataFrame = executor.applyViolations(transformedDataFrame, table.getViolations());
            val postMappingsDataFrame = executor.applyMappings(postViolationsDataFrame, table.getMapping());
            assertEquals(postMappingsDataFrame.schema(), helpers.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void shouldThrowExceptionIfNoSqlDefinedOnTransform() {
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        val transform = new TableDefinition.TransformDefinition();
        transform.setViewText("");

        val inputs = Collections.singletonMap("offenders", helpers.getOffenders(tmp));

        assertThrows(
                DomainExecutorException.class,
                () -> executor.applyTransform(inputs, transform)
        );
    }

    @Test
    public void shouldNotExecuteTransformIfSqlIsBad() throws Exception {
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        val inputs = Collections.singletonMap("offenders", helpers.getOffenders(tmp));

        val transform = new TableDefinition.TransformDefinition();
        transform.setSources(Collections.singletonList("source.table"));
        transform.setViewText("this is bad sql and should fail");

        assertNull(executor.applyTransform(inputs, transform));
    }

    @Test
    public void shouldDeriveNewColumnIfFunctionProvided() throws Exception {
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        val inputs = helpers.getOffenders(tmp);

        val transformSource = "source.table";

        val transform = new TableDefinition.TransformDefinition();
        transform.setViewText(
                "select source.table.*, months_between(current_date(), to_date(source.table.BIRTH_DATE)) / 12 as AGE_NOW " +
                        "from source.table"
        );
        transform.setSources(Collections.singletonList(transformSource));

        val result1 = executor.applyTransform(Collections.singletonMap(transformSource, inputs), transform);

        assertEquals(inputs.count(), result1.count());
        assertFalse(areEqual(inputs, result1));

        transform.setViewText(
                "select a.*, months_between(current_date(), to_date(a.BIRTH_DATE)) / 12 as AGE_NOW " +
                        "from source.table a"
        );

        val result2 = executor.applyTransform(Collections.singletonMap(transformSource, inputs), transform);

        assertEquals(inputs.count(), result2.count());
        assertFalse(areEqual(inputs, result2));
    }

    @Test
    public void shouldNotWriteViolationsIfThereAreNone() throws DataStorageException {
        val sourcePath = this.tmp.toFile().getAbsolutePath() + "/source";
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath, targetPath(), storage, testSchemaService);
        val inputs = helpers.getOffenders(tmp);

        val violation = new TableDefinition.ViolationDefinition();
        violation.setCheck("AGE >= 90");
        violation.setLocation("violations");
        violation.setName("age");

        val outputs = executor.applyViolations(inputs, Collections.singletonList(violation));

        // outputs should be the same as inputs
        assertTrue(this.areEqual(inputs, outputs));
        // there should be no written violations
        assertFalse(storage.exists(
                spark,
                new TableIdentifier(targetPath(), hiveDatabaseName, "violations", "age"))
        );
    }

    @Test
    public void shouldWriteViolationsIfThereAreSome() throws DataStorageException {
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        val violation = new TableDefinition.ViolationDefinition();
        violation.setCheck("AGE <= 18");
        violation.setLocation("violations");
        violation.setName("cannot-vote");

        val inputs = helpers.getOffenders(tmp);
        val outputs = executor.applyViolations(inputs, Collections.singletonList(violation));

        // shouldSubtractViolationsIfThereAreSome
        // outputs should be removed
        assertFalse(areEqual(inputs, outputs));

        // there should be some written violations
        assertTrue(storage.exists(
                spark,
                new TableIdentifier(targetPath(), hiveDatabaseName, "violations", "cannot-vote"))
        );
    }

}
