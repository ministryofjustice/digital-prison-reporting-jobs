package uk.gov.justice.digital.domains;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import uk.gov.justice.digital.client.dynamodb.DynamoDBClient;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.ResourceLoader;
import uk.gov.justice.digital.domains.model.DomainDefinition;
import uk.gov.justice.digital.domains.model.TableDefinition;
import uk.gov.justice.digital.domains.model.TableInfo;
import uk.gov.justice.digital.domains.model.TableTuple;
import uk.gov.justice.digital.domains.service.DomainService;
import uk.gov.justice.digital.exceptions.DomainExecutorException;
import uk.gov.justice.digital.service.DeltaLakeService;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class DomainExecutorTest extends DomainService {

    @TempDir
    Path folder;

    protected static SparkSession getConfiguredSparkSession(SparkConf sparkConf) {
        sparkConf
                .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
                .set("spark.databricks.delta.optimizeWrite.enabled", "true")
                .set("spark.databricks.delta.autoCompact.enabled", "true")
                .set("spark.sql.legacy.charVarcharAsString", "true");

        return SparkSession.builder()
                .config(sparkConf)
                .master("local[*]")
                .getOrCreate();
    }

    @Test
    public void test_getAllSourcesForTable() throws IOException {
        final SparkSession spark = getConfiguredSparkSession(new SparkConf());
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = "test/target/path";
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition);
        List<TableDefinition> tables = domainDefinition.getTables();
        for(final TableDefinition table : tables) {
            TableTuple tuple = new TableTuple("nomis", "offenders");
            System.out.println(tuple.asString());
            for (final String source : table.getTransform().getSources()) {
                final Dataset<Row> dataFrame = executor.getAllSourcesForTable(source, tuple);
                assertNull(dataFrame);
            }
        }
    }

    @Test
    public void test_apply() throws IOException, DomainExecutorException {
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition);
        Map<String, Dataset<Row>> testMap = new HashMap<>();
        testMap.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                getOffenderBookings());
        testMap.put(new TableTuple("nomis", "offenders").asString().toLowerCase(), getOffenders());
        for(final TableDefinition table : tables) {
            Dataset<Row> dataframe = executor.apply(table, testMap);
            dataframe.printSchema();
            assertEquals(dataframe.schema(), createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void test_applyTransform() throws IOException, DomainExecutorException {
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition);
        Map<String, Dataset<Row>> refs = new HashMap<>();
        refs.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                getOffenderBookings());
        refs.put(new TableTuple("nomis", "offenders").asString().toLowerCase(), getOffenders());
        for(final TableDefinition table : tables) {
            final Dataset<Row> transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            assertEquals(transformedDataFrame.schema(), createIncidentDomainDataframe().schema());
        }

    }

    @Test
    public void test_applyViolations() throws IOException, DomainExecutorException {
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition);
        Map<String, Dataset<Row>> refs = new HashMap<>();
        refs.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                getOffenderBookings());
        refs.put(new TableTuple("nomis", "offenders").asString().toLowerCase(), getOffenders());
        for(final TableDefinition table : tables) {
            final Dataset<Row> transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            final Dataset<Row> postViolationsDataFrame = executor.applyViolations(transformedDataFrame, table.getViolations());
            postViolationsDataFrame.printSchema();
            assertEquals(postViolationsDataFrame.schema(), createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void test_applyMappings() throws IOException, DomainExecutorException {
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition);
        Map<String, Dataset<Row>> refs = new HashMap<>();
        refs.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                getOffenderBookings());
        refs.put(new TableTuple("nomis", "offenders").asString().toLowerCase(), getOffenders());
        for(final TableDefinition table : tables) {
            final Dataset<Row> transformedDataFrame = executor.applyTransform(refs, table.getTransform());
            final Dataset<Row> postViolationsDataFrame = executor.applyViolations(transformedDataFrame,
                    table.getViolations());
            final Dataset<Row> postMappingsDataFrame = executor.applyMappings(postViolationsDataFrame,
                    table.getMapping());
            postMappingsDataFrame.printSchema();
            assertEquals(postMappingsDataFrame.schema(), createIncidentDomainDataframe().schema());
        }
    }

    @Test
    public void test_saveFull() throws IOException, DomainExecutorException {
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = folder.toFile().getAbsolutePath() + "/domain/target";
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition);
        Map<String, Dataset<Row>> testMap = new HashMap<>();
        testMap.put(new TableTuple("nomis", "offender_bookings").asString().toLowerCase(),
                getOffenderBookings());
        testMap.put(new TableTuple("nomis", "offenders").asString().toLowerCase(), getOffenders());
        for(final TableDefinition table : tables) {
            Dataset<Row> df_target = executor.apply(table, testMap);
            df_target.printSchema();
            final TableInfo targetInfo = TableInfo.create(targetPath,  domainDefinition.getName(), table.getName());
            executor.saveFull(targetInfo, df_target);
        }
        assertTrue(true);
    }

    @Test
    public void test_deleteFull() throws IOException {
        final SparkSession spark = getConfiguredSparkSession(new SparkConf());
        final String sourcePath = Objects.requireNonNull(getClass().getResource("/sample/events")).getPath();
        final String targetPath = folder.toFile().getAbsolutePath() + "/domain/target";
        System.out.println(targetPath);
        final DomainDefinition domainDefinition = getDomain("/sample/domain/incident_domain.json");
        List<TableDefinition> tables = domainDefinition.getTables();
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domainDefinition);
        for(final TableDefinition table : tables) {
            final TableInfo targetInfo = TableInfo.create(targetPath,  domainDefinition.getName(), table.getName());
            executor.deleteFull(targetInfo);
        }
        assertTrue(true);
    }

    // shouldInitializeDomainExecutor
    @Test
    public void shouldInitializeDomainExecutionJob() throws IOException {
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source-data";
        final String targetPath = "target.path";

        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");
        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);

        assertNotNull(executor);

    }

    // shouldRunWithFullUpdateIfTableIsInDomain
    @Test
    public void shouldRunWithFullUpdateIfTableIsInDomain() throws IOException {
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";

        DeltaLakeService service = new DeltaLakeService();

        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);

        // save a source
        final Dataset<Row> df_offenders = getOffenders();
        saveDataToDisk(TableInfo.create(sourcePath, "source", "table"), df_offenders);

        final String domainOperation = "update";
        // do Full Materialize of source to target
        executor.doFull(domainOperation);

        // there should be a target table
        assertTrue(service.exists(targetPath, "example", "prisoner"));
        // it should have all the offenders in it

        final Dataset<Row> df_refreshed = service.load(targetPath, "example", "prisoner");
        assertTrue(areEqual(df_offenders, df_refreshed));
    }

    // shouldRunWithFullUpdateIfMultipleTablesAreInDomain
    @Test
    public void shouldRunWithFullUpdateIfMultipleTablesAreInDomain() throws IOException {
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";

        DeltaLakeService service = new DeltaLakeService();

        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution-join.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);

        // save a source
        final Dataset<Row> df_offenders = getOffenders();
        saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offenders"), df_offenders);


        final Dataset<Row> df_offenderBookings = getOffenderBookings();
        saveDataToDisk(TableInfo.create(sourcePath, "nomis", "offender_bookings"), df_offenderBookings);


        // do Full Materialize of source to target
        final String domainOperation = "update";
        executor.doFull(domainOperation);
        // there should be a target table
        assertTrue(service.exists(targetPath, "example", "prisoner"));
        // it should have all the joined records in it
        final Dataset<Row> df_refreshed = service.load(targetPath, "example", "prisoner");
        df_refreshed.show();
        // not equal

        // now the reverse
        executor.doFull(domainOperation);
        // there should be a target table
        assertTrue(service.exists(targetPath, "example", "prisoner"));
        // it should have all the joined records in it
        final Dataset<Row> df_refreshed2 = service.load(targetPath, "example", "prisoner");
        df_refreshed2.show();
    }

    // shouldRunWith0ChangesIfTableIsNotInDomain
    @Test
    public void shouldRunWith0ChangesIfTableIsNotInDomain() throws IOException {
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution-bad-source-table.json");

        DeltaLakeService service = new DeltaLakeService();

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
        final Dataset<Row> df_offenders = getOffenders();
        saveDataToDisk(TableInfo.create(sourcePath, "source", "table"), df_offenders);
        final String domainOperation = "insert";
        executor.doFull(domainOperation);

        // there shouldn't be a target table
        assertFalse(service.exists(targetPath, "example", "prisoner"));

    }
// ********************
    // Transform Tests
    // ********************

    // shouldNotExecuteTransformIfNoSqlExists
    @Test
    public void shouldNotExecuteTransformIfNoSqlExists() throws IOException {
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);

        final TableDefinition.TransformDefinition transform = new TableDefinition.TransformDefinition();
        transform.setViewText("");

        Map<String, Dataset<Row>> inputs = new HashMap<>();
        // Add sourceTable if present
        final String sourceTable = "OFFENDERS";
        inputs.put(sourceTable.toLowerCase(), getOffenders());

        try {
            final Dataset<Row> outputs = executor.applyTransform(inputs, transform);
            assertEquals(inputs.get("offenders").count(), outputs.count());
            assertTrue(this.areEqual(inputs.get("offenders"), outputs));
        } catch (DomainExecutorException e) {
            System.err.println("view text is empty");
        }
    }

    // shouldNotExecuteTransformIfSqlIsBad
    @Test
    public void shouldNotExecuteTransformIfSqlIsBad() throws IOException, DomainExecutorException {
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
        Map<String, Dataset<Row>> inputs = new HashMap<>();
        final String sourceTable = "OFFENDERS";
        inputs.put(sourceTable.toLowerCase(), getOffenders());

        final TableDefinition.TransformDefinition transform = new TableDefinition.TransformDefinition();
        transform.setSources(new ArrayList<String>(Collections.singleton("source.table")));
        transform.setViewText("this is bad sql and should fail");

        final Dataset<Row> outputs = executor.applyTransform(inputs, transform);
        assertNull(outputs);
    }
    // shouldDeriveNewColumnIfFunctionProvided
    @Test
    public void shouldDeriveNewColumnIfFunctionProvided() throws IOException {
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
        final Dataset<Row> inputs = getOffenders();

        final TableDefinition.TransformDefinition transform = new TableDefinition.TransformDefinition();
        transform.setViewText("select source.table.*, months_between(current_date()," +
                " to_date(source.table.BIRTH_DATE)) / 12 as AGE_NOW from source.table");
        transform.setSources(new ArrayList<String>(Collections.singleton("source.table")));
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
        final DeltaLakeService service = new DeltaLakeService();
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
        final Dataset<Row> inputs = getOffenders();

        final TableDefinition.ViolationDefinition violation = new TableDefinition.ViolationDefinition();
        violation.setCheck("AGE < 100");
        violation.setLocation("safety");
        violation.setName("age");

        final Dataset<Row> outputs = executor.applyViolations(inputs,
                Collections.<TableDefinition.ViolationDefinition>singletonList(violation));

        // outputs should be the same as inputs
        assertTrue(this.areEqual(inputs, outputs));
        // there should be no written violations
        assertFalse(service.exists(targetPath + "/safety", "violations", "age"));
    }

    // shouldWriteViolationsIfThereAreSome
    // shouldSubtractViolationsIfThereAreSome
    @Test
    public void shouldWriteViolationsIfThereAreSome() throws IOException {
        final DeltaLakeService service = new DeltaLakeService();
        final String sourcePath = folder.toFile().getAbsolutePath() + "/source";
        final String targetPath = folder.toFile().getAbsolutePath() + "/target";
        final DomainDefinition domain = getDomain("/sample/domain/sample-domain-execution.json");

        final DomainExecutor executor = new DomainExecutor(sourcePath, targetPath, domain);
        final Dataset<Row> inputs = getOffenders();

        final TableDefinition.ViolationDefinition violation = new TableDefinition.ViolationDefinition();
        violation.setCheck("AGE >= 100");
        violation.setLocation("violations");
        violation.setName("young");

        final Dataset<Row> outputs = executor.applyViolations(inputs,
                Collections.<TableDefinition.ViolationDefinition>singletonList(violation));

        // shouldSubtractViolationsIfThereAreSome
        // outputs should be removed
        assertFalse(this.areEqual(inputs, outputs));
        assertTrue(outputs.isEmpty());

        // there should be some written violations
        assertTrue(service.exists(targetPath, "violations", "young"));
    }

    // ********************
    // FUNCTIONS
    // ********************


    protected Dataset<Row> doTransform(final DomainExecutor executor, final Dataset<Row> df,
                                       final TableDefinition.TransformDefinition transform, final String source) {
        try {
            Map<String, Dataset<Row>> inputs = new HashMap<>();
            // Add sourceTable if present
//            final String sourceTable = "OFFENDERS";
            inputs.put(source.toLowerCase(), df);
            return executor.applyTransform(inputs, transform);
        } catch (DomainExecutorException e) {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            System.err.print(sw.getBuffer().toString());
        }
        return null;
    }

    protected DomainDefinition getDomain(final String resource) throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final String json = ResourceLoader.getResource(DomainExecutorTest.class, resource);
        return mapper.readValue(json, DomainDefinition.class);
    }

    private void saveDataToDisk(final TableInfo location, final Dataset<Row> df) {
        DeltaLakeService service = new DeltaLakeService();
        service.replace(location.getPrefix(), location.getSchema(), location.getTable(), df);
    }

    protected static InputStream getStream(final String resource) {
        InputStream stream = System.class.getResourceAsStream(resource);
        if(stream == null) {
            stream = System.class.getResourceAsStream("/src/test/resources" + resource);
            if(stream == null) {
                stream = System.class.getResourceAsStream("/target/test-classes" + resource);
                if(stream == null) {
                    Path root = Paths.get(".").normalize().toAbsolutePath();
                    stream = System.class.getResourceAsStream(root.toString() + "/src/test/resources" + resource);
                    if(stream == null) {
                        stream = BaseSparkTest.class.getResourceAsStream(resource);
                    }
                }
            }
        }
        return stream;
    }

    protected Path createFileFromResource(final String resource, final String filename) throws IOException {
        final InputStream stream = getStream(resource);
        final File f = Paths.get(filename).toFile();
        FileUtils.copyInputStreamToFile(stream, f);
        return Paths.get(f.getAbsolutePath());
    }

    protected Dataset<Row> loadParquetDataframe(final String resource, final String filename) throws IOException {
        final SparkSession spark = getConfiguredSparkSession(new SparkConf());
        return spark.read().parquet(createFileFromResource(resource, filename).toString());
    }

    private Dataset<Row> getOffenders() throws IOException {
        return this.loadParquetDataframe("/sample/events/nomis/offenders/offenders.parquet",
                folder.toFile().getAbsolutePath() + "offenders.parquet");

    }

    private Dataset<Row> getOffenderBookings() throws IOException {
        return this.loadParquetDataframe("/sample/events/nomis/offender_bookings/offender-bookings.parquet",
                folder.toFile().getAbsolutePath() + "offender-bookings.parquet");
    }

    @SuppressWarnings("unused")
    private Dataset<Row> getValidDataset() throws IOException {
        return this.loadParquetDataframe("/sample/events/updates.parquet", "updates.parquet");
    }

    protected boolean areEqual(final Dataset<Row> a, final Dataset<Row> b) {
        if(!a.schema().equals(b.schema()))
            return false;
        final List<Row> al = a.collectAsList();
        final List<Row> bl = b.collectAsList();

        if(al == null && bl == null) return true;

        if(al.isEmpty() && bl.isEmpty()) return true;
        if(al.isEmpty() && !bl.isEmpty()) return false;
        if(!al.isEmpty() && bl.isEmpty()) return false;

        return CollectionUtils.subtract(al, bl).size() == 0;
    }

    public Dataset<Row> createIncidentDomainDataframe() {
        final SparkSession spark = getConfiguredSparkSession(new SparkConf());
        List<StructField> fields = new ArrayList<>(2);
        fields.add(DataTypes.createStructField("id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("birth_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("living_unit_id", DataTypes.LongType, true));
        fields.add(DataTypes.createStructField("first_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("last_name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("offender_no", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        return spark.read().schema(schema).json(spark.emptyDataset(Encoders.STRING()));
    }

}
