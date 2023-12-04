package uk.gov.justice.digital.job.batchprocessing;

import lombok.val;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.converter.dms.DMS_3_4_7;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.raw.RawZone;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class BatchProcessorIntegrationTest extends BaseSparkTest {
    @Mock
    private RawZone rawZone;
    @Mock
    private StructuredZoneLoad structuredZoneLoad;
    @Mock
    private StructuredZoneCDC structuredZoneCDC;
    @Mock
    private CuratedZoneLoad curatedZoneLoad;
    @Mock
    private CuratedZoneCDC curatedZoneCDC;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private ViolationService violationService;

    private DMS_3_4_7 converter;

    private static Dataset<Row> createTableRecordDf;
    private static Dataset<Row> loadRecordDf;
    private static Dataset<Row> cdcRecordDf;

    @BeforeAll
    public static void setUpData() {
        createTableRecordDf = getData(DATA_CONTROL_RECORD_PATH);
        loadRecordDf = getData(DATA_LOAD_RECORD_PATH);
        cdcRecordDf = getData(DATA_UPDATE_RECORD_PATH);
    }

    @BeforeEach
    void setUp() {
        converter = new DMS_3_4_7(spark);
    }

    @Test
    public void shouldProcessABatchContainingALoadRecord() throws Exception {
        BatchProcessor undertest = new BatchProcessor(
                rawZone,
                structuredZoneLoad,
                structuredZoneCDC,
                curatedZoneLoad,
                curatedZoneCDC,
                sourceReferenceService,
                violationService
        );
        givenSourceReferenceIsPresent("public", "offenders");

        Dataset<Row> df = populatedDataFrame();

        givenStructuredZoneLoadReturns(df);
        givenStructuredZoneCDCReturns(spark.emptyDataFrame());
        givenCuratedZoneCDCReturns(spark.emptyDataFrame());

        undertest.processBatch(spark, converter, loadRecordDf);

        // There is 1 (table, source, operation) tuple in the input data
        int expectedZoneIterations = 1;

        shouldProcessRawZone(expectedZoneIterations);
        shouldProcessStructuredZoneLoad(expectedZoneIterations);
        shouldProcessStructuredZoneCDC(expectedZoneIterations);
        shouldProcessCuratedZoneLoad(expectedZoneIterations);
        shouldProcessCuratedZoneCDC(expectedZoneIterations);
    }

    @Test
    public void shouldProcessABatchContainingMixedData() throws Exception {
        BatchProcessor undertest = new BatchProcessor(
                rawZone,
                structuredZoneLoad,
                structuredZoneCDC,
                curatedZoneLoad,
                curatedZoneCDC,
                sourceReferenceService,
                violationService
        );
        givenSourceReferenceIsPresent("public", "offenders");
        givenSourceReferenceIsPresent("oms_owner", "agency_internal_locations");
        givenSourceReferenceNotPresent("public", "report_log");

        Dataset<Row> df = populatedDataFrame();

        givenStructuredZoneLoadReturns(df);
        givenStructuredZoneCDCReturns(df);
        givenCuratedZoneCDCReturns(df);

        val inputDf = createTableRecordDf
                .unionAll(loadRecordDf)
                .unionAll(cdcRecordDf);

        undertest.processBatch(spark, converter, inputDf);

        // There are 3 (table, source, operation) tuples in the input data but 1 has no source reference
        int expectedZoneIterations = 2;
        int expectedViolations = 1;

        shouldProcessRawZone(expectedZoneIterations);
        shouldProcessStructuredZoneLoad(expectedZoneIterations);
        shouldProcessStructuredZoneCDC(expectedZoneIterations);
        shouldProcessCuratedZoneLoad(expectedZoneIterations);
        shouldProcessCuratedZoneCDC(expectedZoneIterations);

        shouldAppendViolations(expectedViolations);
    }

    private void givenSourceReferenceIsPresent(String source, String table) {
        val sourceReference = new SourceReference(
                "key",
                source,
                table,
                new SourceReference.PrimaryKey("s"),
                1L,
                new StructType().add(new StructField("s", StringType, true, Metadata.empty()))
        );
        when(sourceReferenceService.getSourceReference(source, table)).thenReturn(Optional.of(sourceReference));
    }
    private void givenSourceReferenceNotPresent(String source, String table) {
        when(sourceReferenceService.getSourceReference(source, table)).thenReturn(Optional.empty());
    }

    private void givenStructuredZoneLoadReturns(Dataset<Row> df) throws DataStorageException {
        when(structuredZoneLoad.process(eq(spark), any(), any())).thenReturn(df);
    }

    private void givenStructuredZoneCDCReturns(Dataset<Row> df) throws DataStorageException {
        when(structuredZoneCDC.process(eq(spark), any(), any())).thenReturn(df);
    }

    private void givenCuratedZoneCDCReturns(Dataset<Row> df) throws DataStorageException {
        when(curatedZoneCDC.process(eq(spark), any(), any())).thenReturn(df);
    }

    private void shouldProcessRawZone(int numTimes) throws DataStorageException {
        verify(rawZone, times(numTimes)).process(eq(spark), any(), any());
    }

    private void shouldProcessStructuredZoneLoad(int numTimes) throws DataStorageException {
        verify(structuredZoneLoad, times(numTimes)).process(eq(spark), any(), any());
    }

    private void shouldProcessStructuredZoneCDC(int numTimes) throws DataStorageException {
        verify(structuredZoneCDC, times(numTimes)).process(eq(spark), any(), any());
    }

    private void shouldProcessCuratedZoneLoad(int numTimes) throws DataStorageException {
        verify(curatedZoneLoad, times(numTimes)).process(eq(spark), any(), any());
    }

    private void shouldProcessCuratedZoneCDC(int numTimes) throws DataStorageException {
        verify(curatedZoneCDC, times(numTimes)).process(eq(spark), any(), any());
    }

    private void shouldAppendViolations(int numTimes) throws DataStorageException {
        verify(violationService, times(numTimes)).handleNoSchemaFound(eq(spark), any(), anyString(), any());
    }

    private Dataset<Row> populatedDataFrame() {
        StructType schema = new StructType().add("some_column", StringType);
        List<String> data = new ArrayList<>();
        data.add("some data");
        JavaRDD<Row> rdd = new JavaSparkContext(spark.sparkContext()).parallelize(data).map(RowFactory::create);
        return spark.sqlContext().createDataFrame(rdd, schema);
    }
}
