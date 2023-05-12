package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import java.util.*;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class StructuredZoneTest extends BaseSparkTest {

    private static final String S3_PATH_KEY = "dpr.structured.s3.path";
    private static final String S3_PATH = "s3://loadjob/structured";

    private final JobParameters jobParameters = new JobParameters(Collections.singletonMap(S3_PATH_KEY, S3_PATH));
    private final DataStorageService storage = new DataStorageService();

    @Mock
    private Dataset<Row> mockedDataSet;

    @Mock
    private Optional<SourceReference> optionalRef;

    MockedStatic<SourceReferenceService> sourceRefStatic;

    @Mock
    private Row mockedRow;


    @BeforeEach
    void setUp() {
        sourceRefStatic = mockStatic(SourceReferenceService.class);
    }

    @AfterEach
    void tearDown() {
        sourceRefStatic.close();
    }

    @Test
    public void shouldReturnValidStructuredS3Path() {
        val source = "oms_owner";
        val table  = "agency_internal_locations";
        val operation = "load";
        val expectedStructuredS3Path = String.join("/", S3_PATH, source, table, operation);
        assertEquals(expectedStructuredS3Path, this.storage.getTablePath(S3_PATH, source, table, operation));
    }

    @Test
    @ExtendWith(MockitoExtension.class)
    public void shouldThrowExceptionWhenViolationsPathNotSet() {
        DataStorageService storage1 = mock(DataStorageService.class);
        assertThrows(IllegalStateException.class, () ->
                new StructuredZone(jobParameters, storage1), "Expected to throw exception, but it didn't"
        );
    }

    @Test
    @ExtendWith(MockitoExtension.class)
    public void shouldHandleValidRecords() {
        // Define a schema for the row
        StructType schema = new StructType()
                .add("source", StringType, false)
                .add("table", StringType, false)
                .add("operation", StringType, false);
        // Create a Row object with key-value pairs
        Row table = new GenericRowWithSchema(Arrays.asList("oms_owner", "agency_internal_locations", "load").toArray(),
                schema);
        SourceReference ref = mock(SourceReference.class);
        JobParameters jobParams = mock(JobParameters.class);
        DataStorageService storage1 = mock(DataStorageService.class);
        doReturn("testPath").when(storage1).getTablePath(S3_PATH, ref);
        sourceRefStatic.when(() -> SourceReferenceService
                        .getSourceReference(table.getAs("source"), table.getAs("table")))
                .thenReturn(optionalRef);
        doReturn(true).when(optionalRef).isPresent();
        doReturn(ref).when(optionalRef).get();

        doReturn(Optional.of("s3://loadjob/violations")).when(jobParams).getViolationsS3Path();
        doReturn(Optional.of("s3://loadjob/structured")).when(jobParams).getStructuredS3Path();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        doReturn(mockedDataSet).when(structuredZoneTest).validateJsonData(any(), any(), any(), any());
        doNothing().when(structuredZoneTest).handleInValidRecords(any(), any(), any(), any(), any());
        doReturn(mockedDataSet).when(structuredZoneTest).handleValidRecords(any(), any(), any());
        when(mockedDataSet.count()).thenReturn(10L);
        Dataset<Row> actual_result = structuredZoneTest.process(spark, mockedDataSet, table);
        assertNotNull(actual_result);
    }

    @Test
    @ExtendWith(MockitoExtension.class)
    public void shouldHandleInValidRecords() {
        // Define a schema for the row
        StructType schema = new StructType()
                .add("source", StringType, false)
                .add("table", StringType, false)
                .add("operation", StringType, false);
        // Create a Row object with key-value pairs
        Row table = new GenericRowWithSchema(Arrays.asList("oms_owner", "agency_internal_locations", "load").toArray(),
                schema);
        JobParameters jobParams = mock(JobParameters.class);
        DataStorageService storage1 = mock(DataStorageService.class);
        sourceRefStatic.when(() -> SourceReferenceService
                        .getSourceReference(table.getAs("source"), table.getAs("table")))
                .thenReturn(optionalRef);
        doReturn(false).when(optionalRef).isPresent();

        doReturn(Optional.of("s3://loadjob/violations")).when(jobParams).getViolationsS3Path();
        doReturn(Optional.of("s3://loadjob/structured")).when(jobParams).getStructuredS3Path();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        doReturn(mockedDataSet).when(structuredZoneTest).handleNoSchemaFound(any(), any(), any(), any());
        when(mockedDataSet.count()).thenReturn(10L);
        Dataset<Row> actual_result = structuredZoneTest.process(spark, mockedDataSet, table);
        assertNotNull(actual_result);
    }

    @Test
    @ExtendWith(MockitoExtension.class)
    public void shouldHandleSchemaFound() {
        DataStorageService storage1 = mock(DataStorageService.class);
        JobParameters jobParams = mock(JobParameters.class);
        doReturn(Optional.of("s3://loadjob/violations")).when(jobParams).getViolationsS3Path();
        doReturn(Optional.of("s3://loadjob/structured")).when(jobParams).getStructuredS3Path();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        Optional<SourceReference> optionalRef = SourceReferenceService.getSourceReference("oms_owner",
                "offenders");
        SourceReference ref = optionalRef.orElse(null);

        if (ref != null) {
            doReturn("s3://loadjob/structured")
                    .doReturn("s3://loadjob/violations").when(storage1)
                    .getTablePath(anyString(), any());
            doReturn(mockedDataSet).when(structuredZoneTest).validateJsonData(mockedDataSet, ref.getSchema(),
                    ref.getSource(), ref.getTable());
            doNothing().when(structuredZoneTest).handleInValidRecords(spark, mockedDataSet, ref.getSource(),
                    ref.getTable(), "s3://loadjob/violations");
            doReturn(mockedDataSet).when(structuredZoneTest).handleValidRecords(spark, mockedDataSet,
                    "s3://loadjob/structured");
            Dataset<Row> actual_result = structuredZoneTest.handleSchemaFound(spark, mockedDataSet, ref);
            assertNotNull(actual_result);
        }
    }

    @Test
    @ExtendWith(MockitoExtension.class)
    public void shouldHandleNoSchemaFound() {
        // Create the StructType object representing the schema
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("data", StringType, true),
                DataTypes.createStructField("metadata", StringType, true),
                DataTypes.createStructField("parsedData", StringType, true),
                DataTypes.createStructField("valid", StringType, true)
        });

        // Create an empty DataFrame with the schema
        Dataset<Row> df = spark.createDataFrame(new ArrayList<>(), schema);
        DataStorageService storage1 = mock(DataStorageService.class);
        JobParameters jobParams = mock(JobParameters.class);
        doReturn(Optional.of("s3://loadjob/violations")).when(jobParams).getViolationsS3Path();
        doReturn(Optional.of("s3://loadjob/structured")).when(jobParams).getStructuredS3Path();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        Optional<SourceReference> optionalRef = SourceReferenceService.getSourceReference("oms_owner",
                "offenders");
        SourceReference ref = optionalRef.orElse(null);
        if (ref != null) {
            Dataset<Row> actual_result = structuredZoneTest.handleNoSchemaFound(spark, df,
                    ref.getSource(), ref.getTable());
            assertNotNull(actual_result);
        }
    }
}