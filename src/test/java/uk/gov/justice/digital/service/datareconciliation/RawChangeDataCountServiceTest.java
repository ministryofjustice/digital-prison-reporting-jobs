package uk.gov.justice.digital.service.datareconciliation;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableCount;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;
import static uk.gov.justice.digital.test.MinimalTestData.manyRowsPerPkDfSameTimestamp;
import static uk.gov.justice.digital.test.MinimalTestData.rowPerPkDfSameTimestamp;

@ExtendWith(MockitoExtension.class)
class RawChangeDataCountServiceTest extends SparkTestBase {

    private static final String RAW_PATH = "s3://raw-bucket/";
    private static final String RAW_ARCHIVE_PATH = "s3://raw-archive-bucket/";
    private static final SourceReference sourceReference1 = new SourceReference(
            "key",
            "namespace",
            "source",
            "table1",
            null,
            "",
            null,
            null
    );
    private static final SourceReference sourceReference2 = new SourceReference(
            "key",
            "namespace",
            "source",
            "table2",
            null,
            "",
            null,
            null
    );

    @Mock
    private JobArguments jobArguments;
    @Mock
    private S3DataProvider s3DataProvider;
    @Mock
    private AnalysisException analysisException;

    @InjectMocks
    private RawChangeDataCountService underTest;

    @Test
    void shouldCombineRawAndRawArchiveCountsByOperation() {
        when(jobArguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(jobArguments.getRawArchiveS3Path()).thenReturn(RAW_ARCHIVE_PATH);

        when(s3DataProvider.getBatchSourceData(spark, RAW_PATH + "source/table1")).thenReturn(inserts(spark));
        when(s3DataProvider.getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table1")).thenReturn(rowPerPkDfSameTimestamp(spark));

        when(s3DataProvider.getBatchSourceData(spark, RAW_PATH + "source/table2")).thenReturn(manyRowsPerPkDfSameTimestamp(spark));
        when(s3DataProvider.getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table2")).thenReturn(manyRowsPerPkDfSameTimestamp(spark));

        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2);
        Map<String, ChangeDataTableCount> result = underTest.changeDataCounts(spark, sourceReferences);

        Map<String, ChangeDataTableCount> expectedResult = new HashMap<>();
        expectedResult.put("source.table1", new ChangeDataTableCount(0.0, 0L, 6L, 1L, 1L));
        expectedResult.put("source.table2", new ChangeDataTableCount(0.0, 0L, 6L, 6L, 6L));

        assertEquals(expectedResult, result);

        verify(s3DataProvider, times(1)).getBatchSourceData(spark, RAW_PATH + "source/table1");
        verify(s3DataProvider, times(1)).getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table1");
        verify(s3DataProvider, times(1)).getBatchSourceData(spark, RAW_PATH + "source/table2");
        verify(s3DataProvider, times(1)).getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table2");
    }

    @Test
    void shouldSetCountsToZeroWhenTablePathDoesNotExist() {
        when(jobArguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(jobArguments.getRawArchiveS3Path()).thenReturn(RAW_ARCHIVE_PATH);

        when(analysisException.getMessage()).thenReturn("Path does not exist");
        // We use thenAnswer instead of thenThrow because Scala treats checked
        // exceptions like AnalysisException as if they are unchecked Exceptions
        when(s3DataProvider.getBatchSourceData(spark, RAW_PATH + "source/table1")).thenAnswer(invocation -> {
            throw analysisException;
        });
        when(s3DataProvider.getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table1")).thenAnswer(invocation -> {
            throw analysisException;
        });

        List<SourceReference> sourceReferences = Collections.singletonList(sourceReference1);
        Map<String, ChangeDataTableCount> result = underTest.changeDataCounts(spark, sourceReferences);

        Map<String, ChangeDataTableCount> expectedResult = new HashMap<>();
        expectedResult.put("source.table1", new ChangeDataTableCount(0.0, 0L, 0L, 0L, 0L));

        assertEquals(expectedResult, result);
    }

    @Test
    void shouldIgnoreUnknownOperations() {
        when(jobArguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(jobArguments.getRawArchiveS3Path()).thenReturn(RAW_ARCHIVE_PATH);

        when(s3DataProvider.getBatchSourceData(spark, RAW_PATH + "source/table1")).thenReturn(withUnknownOperations(spark));
        when(s3DataProvider.getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table1")).thenReturn(withUnknownOperations(spark));

        List<SourceReference> sourceReferences = Collections.singletonList(sourceReference1);
        Map<String, ChangeDataTableCount> result = underTest.changeDataCounts(spark, sourceReferences);

        Map<String, ChangeDataTableCount> expectedResult = new HashMap<>();
        expectedResult.put("source.table1", new ChangeDataTableCount(0.0, 0L, 6L, 0L, 0L));

        assertEquals(expectedResult, result);
    }

    @Test
    void shouldPopulateTolerancesInCurrentStateTableCount() {
        double relativeTolerance = 0.15;
        long absoluteTolerance = 7L;

        when(jobArguments.getRawS3Path()).thenReturn(RAW_PATH);
        when(jobArguments.getRawArchiveS3Path()).thenReturn(RAW_ARCHIVE_PATH);

        when(s3DataProvider.getBatchSourceData(spark, RAW_PATH + "source/table1")).thenReturn(inserts(spark));
        when(s3DataProvider.getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table1")).thenReturn(rowPerPkDfSameTimestamp(spark));

        when(s3DataProvider.getBatchSourceData(spark, RAW_PATH + "source/table2")).thenReturn(manyRowsPerPkDfSameTimestamp(spark));
        when(s3DataProvider.getBatchSourceData(spark, RAW_ARCHIVE_PATH + "source/table2")).thenReturn(manyRowsPerPkDfSameTimestamp(spark));

        when(jobArguments.getReconciliationChangeDataCountsToleranceRelativePercentage()).thenReturn(relativeTolerance);
        when(jobArguments.getReconciliationChangeDataCountsToleranceAbsolute()).thenReturn(absoluteTolerance);

        List<SourceReference> sourceReferences = Arrays.asList(sourceReference1, sourceReference2);
        ChangeDataTableCount result = underTest.changeDataCounts(spark, sourceReferences).get("source.table1");

        assertEquals(relativeTolerance, result.getRelativeTolerance());
        assertEquals(absoluteTolerance, result.getAbsoluteTolerance());
    }

    private static Dataset<Row> withUnknownOperations(SparkSession spark) {
        return spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:50:00.123456", Insert, "1"),
                createRow(2, "2023-11-13 10:50:00.123456", Insert, "2"),
                createRow(3, "2023-11-13 10:50:00.123456", Insert, "3"),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", "unknown operation", "3")
        ), TEST_DATA_SCHEMA);
    }

}
