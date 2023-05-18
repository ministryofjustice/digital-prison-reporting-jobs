package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class StructuredZoneTest extends BaseSparkTest {

    private static final String S3_PATH_KEY = "dpr.structured.s3.path";
    private static final String S3_PATH = "s3://loadjob/structured";

    private final JobArguments jobArguments = new JobArguments(Collections.singletonMap(S3_PATH_KEY, S3_PATH));
    private final DataStorageService storage = new DataStorageService();

    @Mock
    private Dataset<Row> mockedDataSet;

    private MockedStatic<SourceReferenceService> sourceRefStatic;

    @BeforeEach
    void setUp() {
        sourceRefStatic = mockStatic(SourceReferenceService.class);
    }

    @AfterEach
    void tearDown() {
        sourceRefStatic.close();
    }

    @Test
    void shouldReturnValidStructuredS3Path() {
        val source = "oms_owner";
        val table = "agency_internal_locations";
        val operation = "load";
        val expectedStructuredS3Path = String.join("/", S3_PATH, source, table, operation);
        assertEquals(expectedStructuredS3Path, this.storage.getTablePath(S3_PATH, source, table, operation));
    }

    @Test
    void shouldThrowExceptionWhenViolationsPathNotSet() {
        DataStorageService storage1 = mock(DataStorageService.class);
        assertThrows(IllegalStateException.class, () ->
                new StructuredZone(jobArguments, storage1), "Expected to throw exception, but it didn't"
        );
    }

    @Test
    void shouldHandleValidRecords() throws DataStorageException {
        StructType schema = new StructType()
                .add("source", StringType, false)
                .add("table", StringType, false)
                .add("operation", StringType, false);
        Row table = new GenericRowWithSchema(Arrays.asList("oms_owner", "agency_internal_locations", "load").toArray(),
                schema);
        JobArguments jobParams = mock(JobArguments.class);
        DataStorageService storage1 = mock(DataStorageService.class);
        sourceRefStatic.when(() -> SourceReferenceService
                        .generateKey(table.getAs("source"), table.getAs("table")))
                .thenCallRealMethod();
        sourceRefStatic.when(() -> SourceReferenceService
                        .getSourceReference(table.getAs("source"), table.getAs("table")))
                .thenCallRealMethod();
        Optional<SourceReference> optionalSourceRef = SourceReferenceService
                .getSourceReference("oms_owner", "agency_internal_locations");
        val sourceRef = optionalSourceRef.orElse(null);
        assertTrue(optionalSourceRef.isPresent());
        doCallRealMethod().when(storage1).getTablePath(S3_PATH, sourceRef);
        doCallRealMethod().when(storage1).getTablePath(any());
        doReturn("s3://loadjob/violations").when(jobParams).getViolationsS3Path();
        doReturn("s3://loadjob/structured").when(jobParams).getStructuredS3Path();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        doReturn(mockedDataSet).when(structuredZoneTest).validateJsonData(any(), any(), any(), any(), any());
        doNothing().when(structuredZoneTest).handleInValidRecords(any(), any(), any(), any(), any());
        doReturn(mockedDataSet).when(structuredZoneTest).handleValidRecords(spark, mockedDataSet,
                "s3://loadjob/structured/nomis/agency_internal_locations");
        when(mockedDataSet.count()).thenReturn(10L);
        Dataset<Row> actualResult = structuredZoneTest.process(spark, mockedDataSet, table);
        assertNotNull(actualResult);
    }

    @Test
    void shouldHandleInValidRecords() throws DataStorageException {
        StructType schema = new StructType()
                .add("source", StringType, false)
                .add("table", StringType, false)
                .add("operation", StringType, false);
        Row table = new GenericRowWithSchema(Arrays.asList("oms_owner", "agency_internal_locations", "load").toArray(),
                schema);
        JobArguments jobParams = mock(JobArguments.class);
        DataStorageService storage1 = mock(DataStorageService.class);
        sourceRefStatic.when(() -> SourceReferenceService
                        .generateKey(table.getAs("source"), table.getAs("table")))
                .thenCallRealMethod();
        sourceRefStatic.when(() -> SourceReferenceService
                        .getSourceReference(table.getAs("source"), table.getAs("table")))
                .thenCallRealMethod();
        Optional<SourceReference> optionalSourceRef = SourceReferenceService
                .getSourceReference("oms_owner", "agency_internal_locations");
        val sourceRef = optionalSourceRef.orElse(null);
        assertTrue(optionalSourceRef.isPresent());

        doReturn("s3://loadjob/violations").when(jobParams).getViolationsS3Path();
        doReturn("s3://loadjob/structured").when(jobParams).getStructuredS3Path();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        doReturn("s3://loadjob/structured")
                .doReturn("s3://loadjob/violations").when(storage1)
                .getTablePath(anyString(), any());
        doCallRealMethod().when(structuredZoneTest).handleSchemaFound(spark, mockedDataSet, sourceRef);
        doReturn(mockedDataSet).when(structuredZoneTest).validateJsonData(spark, mockedDataSet,
                sourceRef.getSchema(), sourceRef.getSource(), sourceRef.getTable());
        doNothing().when(structuredZoneTest).handleInValidRecords(spark, mockedDataSet, sourceRef.getSource(),
                sourceRef.getTable(), "s3://loadjob/violations");
        doReturn(mockedDataSet).when(structuredZoneTest).handleValidRecords(any(), any(), anyString());
        when(mockedDataSet.count()).thenReturn(10L);
        Dataset<Row> actualResult = structuredZoneTest.process(spark, mockedDataSet, table);
        assertNotNull(actualResult);
    }

    @Test
    void shouldHandleSchemaFound() throws DataStorageException {
        DataStorageService storage1 = mock(DataStorageService.class);
        JobArguments jobParams = mock(JobArguments.class);
        doReturn("s3://loadjob/violations").when(jobParams).getViolationsS3Path();
        doReturn("s3://loadjob/structured").when(jobParams).getStructuredS3Path();
        sourceRefStatic.when(() -> SourceReferenceService
                        .generateKey("oms_owner", "offenders"))
                .thenCallRealMethod();
        sourceRefStatic.when(() -> SourceReferenceService
                        .getSourceReference("oms_owner", "offenders"))
                .thenCallRealMethod();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        Optional<SourceReference> optionalSourceRef = SourceReferenceService
                .getSourceReference("oms_owner", "offenders");
        val sourceRef = optionalSourceRef.orElse(null);
        assertTrue(optionalSourceRef.isPresent());
        doReturn("s3://loadjob/structured")
                .doReturn("s3://loadjob/violations").when(storage1)
                .getTablePath(anyString(), any());
        doCallRealMethod().when(structuredZoneTest).handleSchemaFound(spark, mockedDataSet, sourceRef);
        doReturn(mockedDataSet).when(structuredZoneTest).validateJsonData(spark, mockedDataSet,
                sourceRef.getSchema(), sourceRef.getSource(), sourceRef.getTable());
        doNothing().when(structuredZoneTest).handleInValidRecords(spark, mockedDataSet, sourceRef.getSource(),
                sourceRef.getTable(), "s3://loadjob/violations");
        doReturn(mockedDataSet).when(structuredZoneTest).handleValidRecords(spark, mockedDataSet,
                "s3://loadjob/structured");
        Dataset<Row> actualResult = structuredZoneTest.handleSchemaFound(spark, mockedDataSet,
                sourceRef);
        assertNotNull(actualResult);

    }

    @Test
    void shouldHandleNoSchemaFound() throws DataStorageException {
        // Create the StructType object representing the schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("data", StringType, true),
                DataTypes.createStructField("metadata", StringType, true),
                DataTypes.createStructField("parsedData", StringType, true),
                DataTypes.createStructField("valid", StringType, true)
        });

        // Create an empty DataFrame with the schema
        Dataset<Row> df = spark.createDataFrame(new ArrayList<>(), schema);
        DataStorageService storage1 = mock(DataStorageService.class);
        JobArguments jobParams = mock(JobArguments.class);
        doReturn("s3://loadjob/violations").when(jobParams).getViolationsS3Path();
        doReturn("s3://loadjob/structured").when(jobParams).getStructuredS3Path();
        StructuredZone structuredZoneTest = spy(new StructuredZone(jobParams, storage1));
        sourceRefStatic.when(() -> SourceReferenceService
                        .generateKey("oms_owner", "offenders"))
                .thenCallRealMethod();
        sourceRefStatic.when(() -> SourceReferenceService
                        .getSourceReference("oms_owner", "offenders"))
                .thenCallRealMethod();
        Optional<SourceReference> optionalSourceRef = SourceReferenceService
                .getSourceReference("oms_owner", "offenders");
        val sourceRef = optionalSourceRef.orElse(null);
        assertTrue(optionalSourceRef.isPresent());
        Dataset<Row> actualResult = structuredZoneTest.handleNoSchemaFound(spark, df,
                sourceRef.getSource(), sourceRef.getTable());
        assertNotNull(actualResult);

    }
}