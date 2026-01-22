package uk.gov.justice.digital.zone.structured;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;
import uk.gov.justice.digital.service.metrics.BatchMetrics;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

@ExtendWith(MockitoExtension.class)
class StructuredZoneLoadTest extends SparkTestBase {
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

    private StructuredZoneLoad underTest;

    private static Dataset<Row> df;

    @BeforeAll
    static void setUpClass() {
        df = inserts(spark);
    }

    @BeforeEach
    void setUp() {
        when(arguments.getStructuredS3Path()).thenReturn("s3://structured/path");
        when(sourceReference.getSource()).thenReturn("source");
        when(sourceReference.getTable()).thenReturn("table");
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);

        underTest = new StructuredZoneLoad(arguments, storage, violationService);
    }

    @Test
    void shouldAppendDistinctRecordsToTable() {
        underTest.process(spark, batchMetrics, df, sourceReference);

        verify(storage, times(1)).appendDistinct("s3://structured/path/source/table", df, PRIMARY_KEY);
    }

    @Test
    void shouldReturnDataFrameAfterProcessing() {
        List<Row> result = underTest.process(spark, batchMetrics, df, sourceReference).collectAsList();
        List<Row> expected = df.collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));

    }

    @Test
    void shouldHandleRetriesExhausted() {
        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception());
        doThrow(thrown)
                .when(storage)
                .appendDistinct(any(), any(), any());

        Dataset<Row> result = underTest.process(spark, batchMetrics, df, sourceReference);

        assertTrue(result.isEmpty());

        verify(violationService, times(1))
                .handleRetriesExhausted(any(), any(), eq(df), eq("source"), eq("table"), eq(thrown), eq(ViolationService.ZoneName.STRUCTURED_LOAD));
    }
}
