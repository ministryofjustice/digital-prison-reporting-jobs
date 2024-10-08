package uk.gov.justice.digital.client.s3;

import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaAnalysisException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.DataProviderFailedMergingSchemasException;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3DataProviderTest {

    @Mock
    private JobArguments arguments;
    @Mock
    private SparkSession spark;
    @Mock
    private DataFrameReader dfReader;
    @Mock
    private AnalysisException analysisException;
    @Mock
    private DeltaAnalysisException deltaAnalysisException;

    private S3DataProvider underTest;

    @BeforeEach
    public void setUp() {
        underTest = new S3DataProvider(arguments);
    }

    @Test
    public void getBatchSourceDataShouldCatchAndThrowForFailedMergingSchemaException() {
        // This also tests the case where the outer Exception's cause is null
        SparkException mergeFailedException = new SparkException("Failed merging schema");
        List<String> input = Collections.singletonList("s3://somepath");
        Seq<String> scalaExpectedInput = JavaConverters.asScalaIteratorConverter(input.iterator()).asScala().toSeq();
        when(spark.read()).thenReturn(dfReader);
        when(dfReader.option(anyString(), anyString())).thenReturn(dfReader);
        // Can't use thenThrow because scala does not advertise that it throws the checked exception!
        when(dfReader.parquet(scalaExpectedInput)).thenAnswer(i -> {
            throw mergeFailedException;
        });

        assertThrows(
                DataProviderFailedMergingSchemasException.class,
                () -> underTest.getBatchSourceData(spark, input)
        );
    }

    @Test
    public void getBatchSourceDataShouldCatchAndThrowForFailedMergingSchemaWrappedException() {
        SparkException wrappedMergeFailedException = new SparkException("a message", new SparkException("Failed merging schema"));
        List<String> input = Collections.singletonList("s3://somepath");
        Seq<String> scalaExpectedInput = JavaConverters.asScalaIteratorConverter(input.iterator()).asScala().toSeq();
        when(spark.read()).thenReturn(dfReader);
        when(dfReader.option(anyString(), anyString())).thenReturn(dfReader);
        // Can't use thenThrow because scala does not advertise that it throws the checked exception!
        when(dfReader.parquet(scalaExpectedInput)).thenAnswer(i -> {
            throw wrappedMergeFailedException;
        });

        assertThrows(
                DataProviderFailedMergingSchemasException.class,
                () -> underTest.getBatchSourceData(spark, input)
        );
    }

    @Test
    void isPathDoesNotExistExceptionShouldReturnTrueWhenPathDoesNotExist() {
        when(analysisException.getMessage()).thenReturn("Path does not exist");
        assertTrue(S3DataProvider.isPathDoesNotExistException(analysisException));
    }

    @Test
    void isPathDoesNotExistExceptionShouldReturnTrueWhenDeltaPathDoesNotExist() {
        when(deltaAnalysisException.getMessage()).thenReturn("Delta table `s3://some-bucket/some-path` doesn't exist.");
        assertTrue(S3DataProvider.isPathDoesNotExistException(deltaAnalysisException));
    }

    @Test
    void isPathDoesNotExistExceptionShouldReturnFalseWhenOtherReason() {
        when(analysisException.getMessage()).thenReturn("Some other message");
        assertFalse(S3DataProvider.isPathDoesNotExistException(analysisException));
    }

    @Test
    void isPathDoesNotExistExceptionShouldReturnFalseWhenOtherExceptionType() {
        assertFalse(S3DataProvider.isPathDoesNotExistException(new Exception()));
    }
}