package uk.gov.justice.digital.zone.curated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.metrics.BatchMetrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;

@ExtendWith(MockitoExtension.class)
class CuratedZoneLoadTest {

    @Mock
    private SparkSession spark;
    @Mock
    private Dataset<Row> df;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private JobArguments arguments;
    @Mock
    private DataStorageService storage;
    @Mock
    private ViolationService violationService;
    @Mock
    private BatchMetrics batchMetrics;

    private CuratedZoneLoad underTest;

    @BeforeEach
    void setUp() {
        when(arguments.getCuratedS3Path()).thenReturn("s3://curated/path");
        when(sourceReference.getSource()).thenReturn("source");
        when(sourceReference.getTable()).thenReturn("table");
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);

        underTest = new CuratedZoneLoad(arguments, storage, violationService);
    }

    @Test
    void shouldAppendDistinctRecordsToTable() {
        underTest.process(spark, batchMetrics, df, sourceReference);

        verify(storage, times(1)).appendDistinct("s3://curated/path/source/table", df, PRIMARY_KEY);
    }

    @Test
    void shouldUpdateDeltaManifest() {
        underTest.process(spark, batchMetrics, df, sourceReference);

        verify(storage, times(1)).updateDeltaManifestForTable(any(), eq("s3://curated/path/source/table"));
    }

    @Test
    void shouldHandleRetriesExhausted() {
        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception());
        doThrow(thrown)
                .when(storage)
                .appendDistinct(any(), any(), any());

        underTest.process(spark, batchMetrics, df, sourceReference);

        verify(violationService, times(1))
                .handleRetriesExhausted(any(), any(), eq(df), eq("source"), eq("table"), eq(thrown), eq(ViolationService.ZoneName.CURATED_LOAD));
    }

}
