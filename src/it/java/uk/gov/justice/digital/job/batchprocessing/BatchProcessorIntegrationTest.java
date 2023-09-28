package uk.gov.justice.digital.job.batchprocessing;

import lombok.val;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.converter.dms.DMS_3_4_7;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.DomainService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.zone.curated.CuratedZoneCDC;
import uk.gov.justice.digital.zone.curated.CuratedZoneLoad;
import uk.gov.justice.digital.zone.raw.RawZone;
import uk.gov.justice.digital.zone.structured.StructuredZoneCDC;
import uk.gov.justice.digital.zone.structured.StructuredZoneLoad;

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
    private DomainService domainService;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private DataStorageService storageService;
    @Mock
    private JobArguments arguments;

    private DMS_3_4_7 converter;

    private BatchProcessor undertest;

    @BeforeEach
    void setUp() {
        converter = new DMS_3_4_7(spark);
        undertest = new BatchProcessor(
                arguments,
                rawZone,
                structuredZoneLoad,
                structuredZoneCDC,
                curatedZoneLoad,
                curatedZoneCDC,
                domainService,
                sourceReferenceService,
                storageService
        );
    }

    @Test
    public void shouldProcessABatchContainingALoadRecord() throws Exception {
        givenSourceReferenceIsPresent("public", "offenders");
        // Returning empty dataframes is not entirely realistic but does the job for what we want to test
        givenStructuredZoneLoadReturnsEmptyDataFrame();
        givenStructuredZoneCDCReturnsEmptyDataFrame();
        givenCuratedZoneCDCReturnsEmptyDataFrame();

        val inputDf = getData(DATA_LOAD_RECORD_PATH);

        undertest.processBatch(spark, converter, inputDf);

        // There is only one table, source, operation tuple in the input data
        int expectedIterations = 1;

        shouldProcessRawZone(expectedIterations);
        shouldProcessStructuredZoneLoad(expectedIterations);
        shouldProcessStructuredZoneCDC(expectedIterations);
        shouldProcessCuratedZoneLoad(expectedIterations);
        shouldProcessCuratedZoneCDC(expectedIterations);
    }

    @Test
    public void shouldProcessABatchContainingMixedData() throws Exception {
        givenSourceReferenceIsPresent("public", "offenders");
        givenSourceReferenceIsPresent("oms_owner", "agency_internal_locations");
        givenSourceReferenceNotPresent("public", "report_log");
        // Returning empty dataframes is not entirely realistic but does the job for what we want to test
        givenStructuredZoneLoadReturnsEmptyDataFrame();
        givenStructuredZoneCDCReturnsEmptyDataFrame();
        givenCuratedZoneCDCReturnsEmptyDataFrame();

        val inputDf = getData(DATA_CONTROL_RECORD_PATH)
                .unionAll(getData(DATA_LOAD_RECORD_PATH))
                .unionAll(getData(DATA_UPDATE_RECORD_PATH));

        undertest.processBatch(spark, converter, inputDf);

        // There are 3 (table, source, operation) tuples in the input data but 1 has no source reference
        int expectedIterations = 2;
        int expectedViolations = 1;

        shouldProcessRawZone(expectedIterations);
        shouldProcessStructuredZoneLoad(expectedIterations);
        shouldProcessStructuredZoneCDC(expectedIterations);
        shouldProcessCuratedZoneLoad(expectedIterations);
        shouldProcessCuratedZoneCDC(expectedIterations);

        shouldAppendViolations(expectedViolations);
    }

    private void givenSourceReferenceIsPresent(String source, String table) {
        val sourceReference = new SourceReference(
                "key",
                source,
                table,
                new SourceReference.PrimaryKey("s"),
                new StructType().add(new StructField("s", StringType, true, Metadata.empty()))
        );
        when(sourceReferenceService.getSourceReference(source, table)).thenReturn(Optional.of(sourceReference));
    }
    private void givenSourceReferenceNotPresent(String source, String table) {
        when(sourceReferenceService.getSourceReference(source, table)).thenReturn(Optional.empty());
    }

    private void givenStructuredZoneLoadReturnsEmptyDataFrame() throws DataStorageException {;
        when(structuredZoneLoad.process(eq(spark), any(), any())).thenReturn(spark.emptyDataFrame());
    }

    private void givenStructuredZoneCDCReturnsEmptyDataFrame() throws DataStorageException {;
        when(structuredZoneCDC.process(eq(spark), any(), any())).thenReturn(spark.emptyDataFrame());
    }

    private void givenCuratedZoneCDCReturnsEmptyDataFrame() throws DataStorageException {;
        when(curatedZoneCDC.process(eq(spark), any(), any())).thenReturn(spark.emptyDataFrame());
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
        verify(storageService, times(numTimes)).append(anyString(), any());
        verify(storageService, times(numTimes)).updateDeltaManifestForTable(eq(spark), anyString());
    }
}
