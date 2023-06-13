package uk.gov.justice.digital.domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DomainSchemaException;
import uk.gov.justice.digital.test.ResourceLoader;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition;
import uk.gov.justice.digital.domain.model.TableIdentifier;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DomainExecutorException;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainSchemaService;
import uk.gov.justice.digital.test.SparkTestHelpers;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class DomainExecutorTest extends BaseSparkTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final SparkTestHelpers helpers = new SparkTestHelpers(spark);
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();
    private static final DataStorageService storage = new DataStorageService();
    private static final String hiveDatabaseName = "test_db";

    @TempDir
    private Path tmp;

    private static final String SAMPLE_EVENTS_PATH =
            Objects.requireNonNull(
                    DomainExecutorTest.class.getResource("/sample/events")
            ).getPath();

    @Mock
    private Dataset<Row> mockedDataSet;

    private final DataStorageService testStorage = mock(DataStorageService.class);


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
    public void shouldTestApplyTransform() throws Exception {
        val domainDefinition = getDomain("/sample/domain/incident_domain.json");
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), storage, testSchemaService);

        for (val table : domainDefinition.getTables()) {
            val transformedDataFrame = executor.applyTransform(getOffenderRefs(), table.getTransform());
            assertEquals(transformedDataFrame.schema(), helpers.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void shouldTestApplyViolationsIfEmpty() throws Exception {
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
    public void shouldTestApplyViolationsIfNonEmpty() throws Exception {
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
    public void shouldTestApplyMappings() throws Exception {
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
    public void shouldTestSaveTable() throws Exception {
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
    public void shouldTestOverwriteWhenTableExists()
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
    public void shouldTestCreateSchemaAndTable() throws DomainExecutorException, DomainSchemaException {
        TableIdentifier tbl = new TableIdentifier("s3://somepath", "test",
                "incident", "demographics" );
        doReturn(false).when(testStorage).exists(spark, tbl);
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(SAMPLE_EVENTS_PATH, domainTargetPath(), testStorage, testSchemaService);
        executor.insertTable(tbl, mockedDataSet);
        verify(testSchemaService, times(1)).create(any(), any(), any());
    }

    @Test
    public void shouldTestInsertTableIfTableDoesNotExists() throws DomainExecutorException, DomainSchemaException {
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
    public void shouldTestUpdateTableIfTableExists() throws DomainExecutorException, DomainSchemaException {
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
    public void shouldTestSyncTableIfTableExists() throws DomainExecutorException, DataStorageException {
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
    public void shouldTestDeleteTableIfTableExists() throws DomainExecutorException, DomainSchemaException {
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
    public void shouldTestDeleteTable() throws Exception {
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

    @Test
    public void shouldTestRunWithFullUpdateIfTableIsInDomain() throws Exception {
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
    public void shouldTestRunWithFullUpdateIfMultipleTablesAreInDomain() throws Exception {
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
    public void shouldTestRunWithNoChangesIfTableIsNotInDomain() throws Exception {

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

    @Test
    public void shouldTestThrowExceptionIfNoSqlDefinedOnTransform() {
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
    public void shouldTestNotExecuteTransformIfSqlIsBad() throws Exception {
        val testSchemaService = mock(DomainSchemaService.class);
        val executor = createExecutor(sourcePath(), targetPath(), storage, testSchemaService);

        val inputs = Collections.singletonMap("offenders", helpers.getOffenders(tmp));

        val transform = new TableDefinition.TransformDefinition();
        transform.setSources(Collections.singletonList("source.table"));
        transform.setViewText("this is bad sql and should fail");

        assertNull(executor.applyTransform(inputs, transform));
    }

    @Test
    public void shouldTestDeriveNewColumnIfFunctionProvided() throws Exception {
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
    public void shouldTestNotWriteViolationsIfThereAreNone() throws DataStorageException {
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
    public void shouldTestWriteViolationsIfThereAreSome() throws DataStorageException {
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

    private DomainDefinition getDomain(String resource) throws IOException {
        return mapper.readValue(ResourceLoader.getResource(resource), DomainDefinition.class);
    }

    private boolean areEqual(Dataset<Row> a, Dataset<Row> b) {
        return a.schema().equals(b.schema()) &&
                Arrays.equals(a.collectAsList().toArray(), b.collectAsList().toArray());
    }

    private DomainExecutor createExecutor(String source, String target, DataStorageService storage,
                                          DomainSchemaService schemaService) {
        val mockJobParameters = mock(JobArguments.class);
        when(mockJobParameters.getCuratedS3Path()).thenReturn(source);
        when(mockJobParameters.getDomainTargetPath()).thenReturn(target);
        when(mockJobParameters.getDomainCatalogDatabaseName()).thenReturn(hiveDatabaseName);
        return new DomainExecutor(mockJobParameters, storage, schemaService, sparkSessionProvider);
    }

    private String domainTargetPath() {
        return tmp.toFile().getAbsolutePath() + "/domain/target";
    }

    private String targetPath() {
        return tmp.toFile().getAbsolutePath() + "/target";
    }

    private String sourcePath() {
        return tmp.toFile().getAbsolutePath() + "/source";
    }

    private Map<String, Dataset<Row>> getOffenderRefs() {
        val refs = new HashMap<String, Dataset<Row>>();
        refs.put("nomis.offender_bookings", helpers.getOffenderBookings(tmp));
        refs.put("nomis.offenders", helpers.getOffenders(tmp));
        return refs;
    }

}
