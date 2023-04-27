package uk.gov.justice.digital.domain;

import com.amazonaws.services.glue.AWSGlueClientBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.config.ResourceLoader;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.domain.model.TableDefinition;
import uk.gov.justice.digital.domain.model.TableInfo;
import uk.gov.justice.digital.domain.model.TableTuple;
import uk.gov.justice.digital.exception.DomainExecutorException;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.gov.justice.digital.service.DataCatalogService;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.TestUtil;

import java.io.*;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DomainExecutorTest extends BaseSparkTest{

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainExecutorTest.class);

    private static TestUtil utils = null;

    @TempDir
    private Path folder;

    @BeforeAll
    public static void setUp() {
        //instantiate and populate the dependencies
        utils = new TestUtil();
    }

    @Test
    public void test_tempFolder() {
        assertNotNull(this.folder);
    }

    @Test
    public void test_getAllSourcesForTable() throws IOException, DomainExecutorException {
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final DataStorageService storage = new DataStorageService();
        final String targetPath = "test/target/path";
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        List<TableDefinition> tables = domainDefinition.getTables();

        final Dataset<Row> df_offender_bookings = utils.getOffenderBookings(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offender_bookings"),
                df_offender_bookings);

        final Dataset<Row> df_offenders = utils.getOffenders(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offenders"),
                df_offenders);

        for(final TableDefinition table : tables) {
            for (final String source : table.getTransform().getSources()) {
                final Dataset<Row> dataFrame = executor.getAllSourcesForTable(sourcePath, source, null);
               assertNotNull(dataFrame);
            }
        }
    }

    @Test
    public void test_apply() throws IOException, DomainExecutorException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        Map<String, Dataset<Row>> testMap = new HashMap<>();
        testMap.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                utils.getOffenderBookings(folder));
        testMap.put(new TableTuple("nomis", "offenders").asString().toLowerCase(),
                utils.getOffenders(folder));
        for(final TableDefinition table : tables) {
            Dataset<Row> dataframe = executor.apply(table, testMap);
            dataframe.printSchema();
            assertEquals(dataframe.schema(), utils.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void test_applyTransform() throws IOException, DomainExecutorException  {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        Map<String, Dataset<Row>> refs = new HashMap<>();
        refs.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                utils.getOffenderBookings(folder));
        refs.put(new TableTuple("nomis", "offenders").asString().toLowerCase(),
                utils.getOffenders(folder));
        for(final TableDefinition table : tables) {
            final Dataset<Row> transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            assertEquals(transformedDataFrame.schema(), utils.createIncidentDomainDataframe().schema());
        }

    }

    @Test
    public void test_applyViolations_empty() throws IOException, DomainExecutorException {

        final DataStorageService storage = new DataStorageService();
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        Map<String, Dataset<Row>> refs = new HashMap<>();
        refs.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                utils.getOffenderBookings(folder));
        refs.put(new TableTuple("nomis", "offenders").asString().toLowerCase(),
                utils.getOffenders(folder));
        for(final TableDefinition table : tables) {
            final Dataset<Row> transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            final Dataset<Row> postViolationsDataFrame = executor.applyViolations(transformedDataFrame, table.getViolations());
            postViolationsDataFrame.printSchema();
            assertEquals(postViolationsDataFrame.schema(), utils.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void test_applyViolations_nonempty() throws IOException, DomainExecutorException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/domain-violations-check.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        Map<String, Dataset<Row>> refs = new HashMap<>();
        refs.put(new TableTuple("source", "table").asString().toLowerCase(),
                utils.getOffenders(folder));

        for(final TableDefinition table : tables) {
            final Dataset<Row> transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            final Dataset<Row> postViolationsDataFrame = executor.applyViolations(transformedDataFrame,
                    table.getViolations());
            postViolationsDataFrame.printSchema();
            assertEquals(postViolationsDataFrame.schema(), utils.createViolationsDomainDataframe().schema());
        }
    }


    @Test
    public void test_applyMappings() throws IOException, DomainExecutorException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        Map<String, Dataset<Row>> refs = new HashMap<>();
        refs.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                utils.getOffenderBookings(folder));
        refs.put(new TableTuple("nomis", "offenders").asString().toLowerCase(),
                utils.getOffenders(folder));
        for(final TableDefinition table : tables) {
            final Dataset<Row> transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            final Dataset<Row> postViolationsDataFrame = executor.applyViolations(transformedDataFrame,
                    table.getViolations());
            final Dataset<Row> postMappingsDataFrame = executor.applyMappings(postViolationsDataFrame,
                    table.getMapping());
            postMappingsDataFrame.printSchema();
            assertEquals(postMappingsDataFrame.schema(), utils.createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void test_saveFull() throws IOException, DomainExecutorException {
        final DataStorageService storage = new DataStorageService();
        final String domainOperation = "insert";
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        Map<String, Dataset<Row>> testMap = new HashMap<>();
        testMap.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                utils.getOffenderBookings(folder));
        testMap.put(new TableTuple("nomis", "offenders").asString().toLowerCase(),
                utils.getOffenders(folder));
        for(final TableDefinition table : tables) {
            Dataset<Row> df_target = executor.apply(table, testMap);
            df_target.printSchema();
            final TableInfo targetInfo = TableInfo.create(targetPath,  domainDefinition.getName(), table.getName());
            executor.saveFull(targetInfo, df_target, domainOperation);
        }
        assertTrue(true);
    }

    @Test
    public void test_deleteFull() throws IOException, DomainExecutorException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition, storage,
                AWSGlueClientBuilder.defaultClient());
        // Insert first
        final String domainTableName = "demographics";
        executor.doFull(domainDefinition.getName(), domainTableName, "insert");

        for(final TableDefinition table : tables) {
            final TableInfo targetInfo = TableInfo.create(targetPath,  domainDefinition.getName(), table.getName());
            executor.deleteFull(targetInfo);
        }
        assertTrue(true);
    }

    // shouldInitializeDomainExecutor
    @Test
    public void shouldInitializeDomainExecutionJob() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source-data";
        final String targetPath = "target.path";

        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());

        assertNotNull(executor);

    }

    // shouldRunWithFullUpdateIfTableIsInDomain
    @Test
    public void shouldRunWithFullUpdateIfTableIsInDomain() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";

        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());

        // save a source
        final Dataset<Row> df_offenders = utils.getOffenders(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "source", "table"), df_offenders);
        final String domainTableName = "prisoner";
        // Insert first
        executor.doFull(domain.getName(), domainTableName, "insert");
        // then update
        executor.doFull(domain.getName(), domainTableName, "update");


        // there should be a target table
        TableInfo info = TableInfo.create(targetPath, "example", "prisoner");
        assertTrue(storage.exists(spark, info));
        // it should have all the offenders in it

        final Dataset<Row> df_refreshed = storage.load(spark, info);
        assertTrue(areEqual(df_offenders, df_refreshed));
    }

    // shouldRunWithFullUpdateIfMultipleTablesAreInDomain
    @Test
    public void shouldRunWithFullUpdateIfMultipleTablesAreInDomain() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";


        final DomainDefinition domain1 = getDomain("/sample/domain/sample-domain-execution-insert.json");
        final DomainDefinition domain2 = getDomain("/sample/domain/sample-domain-execution-join.json");

        // save a source
        final Dataset<Row> df_offenders = utils.getOffenders(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offenders"), df_offenders);


        final Dataset<Row> df_offenderBookings = utils.getOffenderBookings(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offender_bookings"), df_offenderBookings);


        // do Full Materialize of source to target
        final String domainTableName = "prisoner";
        final DomainExecutor executor1 = new DomainExecutor(sourcePath, targetPath, domain1, storage,
                AWSGlueClientBuilder.defaultClient());
        executor1.doFull(domain1.getName(), domainTableName, "insert");

        // there should be a target table
        TableInfo info = TableInfo.create(targetPath, "example", "prisoner");
        assertTrue(storage.exists(spark, info));
        // it should have all the joined records in it
        final Dataset<Row> df_refreshed = storage.load(spark, info);
        df_refreshed.show();
        // not equal


        // now the reverse
        final DomainExecutor executor2 = new DomainExecutor(sourcePath, targetPath, domain2, storage,
                AWSGlueClientBuilder.defaultClient());
        executor2.doFull(domain2.getName(), domainTableName, "update");

        // there should be a target table
        assertTrue(storage.exists(spark, info));
        // it should have all the joined records in it
        final Dataset<Row> df_refreshed_update = storage.load(spark, info);
        df_refreshed_update.show();
    }

    // shouldRunWith0ChangesIfTableIsNotInDomain
    @Test
    public void shouldRunWith0ChangesIfTableIsNotInDomain() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution-bad-source-table.json");



        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());
        final Dataset<Row> df_offenders = utils.getOffenders(folder);
        utils.saveDataToDisk(TableInfo.create(sourcePath, "source", "table"), df_offenders);
        final String domainOperation = "insert";

        final String domainTableName = "prisoner";
        executor.doFull(domain.getName(), domainTableName, domainOperation);


        // there shouldn't be a target table
        TableInfo info = TableInfo.create(targetPath, "example", "prisoner");
        assertFalse(storage.exists(spark, info));

    }
// ********************
    // Transform Tests
    // ********************

    // shouldNotExecuteTransformIfNoSqlExists
    @Test
    public void shouldNotExecuteTransformIfNoSqlExists() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());

        final TableDefinition.TransformDefinition transform = new TableDefinition.TransformDefinition();
        transform.setViewText("");

        Map<String, Dataset<Row>> inputs = new HashMap<>();
        // Add sourceTable if present
        final String sourceTable = "OFFENDERS";
        inputs.put(sourceTable.toLowerCase(), utils.getOffenders(folder));

        try {
            final Dataset<Row> outputs = executor.applyTransform(inputs, transform);
            assertEquals(inputs.get("offenders").count(), outputs.count());
            assertTrue(this.areEqual(inputs.get("offenders"), outputs));
        } catch (DomainExecutorException e) {
            logger.error("view text is empty");
        }
    }

    // shouldNotExecuteTransformIfSqlIsBad
    @Test
    public void shouldNotExecuteTransformIfSqlIsBad() throws IOException, DomainExecutorException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());
        Map<String, Dataset<Row>> inputs = new HashMap<>();
        final String sourceTable = "OFFENDERS";
        inputs.put(sourceTable.toLowerCase(), utils.getOffenders(folder));

        final TableDefinition.TransformDefinition transform = new TableDefinition.TransformDefinition();
        transform.setSources(new ArrayList<>(Collections.singleton("source.table")));
        transform.setViewText("this is bad sql and should fail");

        final Dataset<Row> outputs = executor.applyTransform(inputs, transform);
        assertNull(outputs);
    }
    // shouldDeriveNewColumnIfFunctionProvided
    @Test
    public void shouldDeriveNewColumnIfFunctionProvided() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());
        final Dataset<Row> inputs = utils.getOffenders(folder);

        final TableDefinition.TransformDefinition transform = new TableDefinition.TransformDefinition();
        transform.setViewText("select source.table.*, months_between(current_date()," +
                " to_date(source.table.BIRTH_DATE)) / 12 as AGE_NOW from source.table");
        transform.setSources(new ArrayList<>(Collections.singleton("source.table")));
        Dataset<Row> outputs = doTransform(executor, inputs, transform, "source.table");

        assertEquals(inputs.count(), outputs.count());
        assertFalse(this.areEqual(inputs, outputs));

        transform.setViewText("select a.*, months_between(current_date(), " +
                "to_date(a.BIRTH_DATE)) / 12 as AGE_NOW from source.table a");
        outputs = doTransform(executor, inputs, transform, "source.table");

        outputs.toDF().show();

        assertEquals(inputs.count(), outputs.count());
        assertFalse(this.areEqual(inputs, outputs));

    }

    // ********************
    // Violation Tests
    // ********************
    // shouldNotWriteViolationsIfThereAreNone
    @Test
    public void shouldNotWriteViolationsIfThereAreNone() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());
        final Dataset<Row> inputs = utils.getOffenders(folder);

        final TableDefinition.ViolationDefinition violation = new TableDefinition.ViolationDefinition();
        violation.setCheck("AGE < 100");
        violation.setLocation("safety");
        violation.setName("age");

        final Dataset<Row> outputs = executor.applyViolations(inputs,
                Collections.singletonList(violation));

        // outputs should be the same as inputs
        assertTrue(this.areEqual(inputs, outputs));
        // there should be no written violations
        assertFalse(storage.exists(spark, TableInfo.create(targetPath + "/safety", "violations", "age")));
    }

    // shouldWriteViolationsIfThereAreSome
    // shouldSubtractViolationsIfThereAreSome
    @Test
    public void shouldWriteViolationsIfThereAreSome() throws IOException {
        final DataStorageService storage = new DataStorageService();
        final String sourcePath = this.folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = this.folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain, storage,
                AWSGlueClientBuilder.defaultClient());
        final Dataset<Row> inputs = utils.getOffenders(folder);

        final TableDefinition.ViolationDefinition violation = new TableDefinition.ViolationDefinition();
        violation.setCheck("AGE >= 100");
        violation.setLocation("violations");
        violation.setName("young");

        final Dataset<Row> outputs = executor.applyViolations(inputs,
                Collections.singletonList(violation));

        // shouldSubtractViolationsIfThereAreSome
        // outputs should be removed
        assertFalse(this.areEqual(inputs, outputs));
        assertTrue(outputs.isEmpty());

        // there should be some written violations
        assertTrue(storage.exists(spark, TableInfo.create(targetPath, "violations", "young")));
    }

    // ********************
    // FUNCTIONS
    // ********************


    protected Dataset<Row> doTransform(final DomainExecutor executor, final Dataset<Row> df,
                                       final TableDefinition.TransformDefinition transform, final String source) {
        try {
            Map<String, Dataset<Row>> inputs = new HashMap<>();
            // Add sourceTable if present
            inputs.put(source.toLowerCase(), df);
            return executor.applyTransform(inputs, transform);
        } catch (DomainExecutorException e) {
            logger.error("Apply transform failed", e);
        }
        return null;
    }

    protected DomainDefinition getDomain(final String resource) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final String json = ResourceLoader.getResource(DomainExecutorTest.class, resource);
        return mapper.readValue(json, DomainDefinition.class);
    }



    protected boolean areEqual(final Dataset<Row> a, final Dataset<Row> b) {
        if(!a.schema().equals(b.schema()))
            return false;
        final List<Row> al = a.collectAsList();
        final List<Row> bl = b.collectAsList();

        if(al == null && bl == null) return true;

        assert al != null;
        if(al.isEmpty() && bl.isEmpty()) return true;
        if(al.isEmpty() && !bl.isEmpty()) return false;
        if(!al.isEmpty() && bl.isEmpty()) return false;

        return CollectionUtils.subtract(al, bl).size() == 0;
    }

}
