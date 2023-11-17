package uk.gov.justice.digital.zone.structured;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;

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
class StructuredZoneLoadS3Test extends BaseSparkTest {
    @Mock
    private SourceReference sourceReference;
    @Mock
    private JobArguments arguments;
    @Mock
    private DataStorageService storage;
    @Mock
    private ViolationService violationService;

    private StructuredZoneLoadS3 underTest;

    private static Dataset<Row> df;

    @BeforeAll
    public static void setUpClass() {
        df = inserts(spark);
    }

    @BeforeEach
    public void setUp() {
        when(arguments.getStructuredS3Path()).thenReturn("s3://structured/path");
        when(sourceReference.getSource()).thenReturn("source");
        when(sourceReference.getTable()).thenReturn("table");
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);

        underTest = new StructuredZoneLoadS3(arguments, storage, violationService);
    }

    @Test
    public void shouldAppendDistinctRecordsToTable() throws DataStorageException {
        underTest.process(spark, df, sourceReference);

        verify(storage, times(1)).appendDistinct(eq("s3://structured/path/source/table"), eq(df), eq(PRIMARY_KEY));
    }

    @Test
    public void shouldReturnDataFrameAfterProcessing() throws DataStorageException {
        List<Row> result = underTest.process(spark, df, sourceReference).collectAsList();
        List<Row> expected = df.collectAsList();

        assertEquals(expected.size(), result.size());
        assertTrue(result.containsAll(expected));

    }

    @Test
    public void shouldHandleRetriesExhausted() throws DataStorageException {
        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception());
        doThrow(thrown)
                .when(storage)
                .appendDistinct(any(), any(), any());

        Dataset<Row> result = underTest.process(spark, df, sourceReference);

        assertTrue(result.isEmpty());

        verify(violationService, times(1))
                .handleRetriesExhaustedS3(any(), eq(df), eq("source"), eq("table"), eq(thrown), eq(ViolationService.ZoneName.STRUCTURED_LOAD));
    }
}