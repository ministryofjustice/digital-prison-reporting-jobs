package uk.gov.justice.digital.zone;


import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RawZoneTest extends BaseSparkTest {

    private static final String S3_PATH_KEY = "dpr.raw.s3.path";
    private static final String S3_PATH = "s3://loadjob/raw";

    private final JobArguments jobArguments = new JobArguments(Collections.singletonMap(S3_PATH_KEY, S3_PATH));
    private final DataStorageService storage = new DataStorageService();

    private final RawZone underTest = new RawZone(jobArguments, storage);

    @Test
    void shouldReturnValidRawS3Path() {
        val source = "oms_owner";
        val table = "agency_internal_locations";
        val operation = "load";
        val expectedRawS3Path = String.join("/", S3_PATH, source, table, operation);
        assertEquals(expectedRawS3Path, this.storage.getTablePath(S3_PATH, source, table, operation));
    }

    @Test
    void shouldProcessRawZone() throws DataStorageException {
        // Define a schema for the row
        StructType schema = new StructType()
                .add("source", StringType, false)
                .add("table", StringType, false)
                .add("operation", StringType, false);
        // Create a Row object with key-value pairs
        Row table = new GenericRowWithSchema(Arrays.asList("oms_owner", "agency_internal_locations", "load").toArray(),
                schema);
        SourceReference ref = mock(SourceReference.class);
        MockedStatic<SourceReferenceService> service = mockStatic(SourceReferenceService.class);
        DataStorageService storage1 = mock(DataStorageService.class);
        doNothing().when(storage1).append("testPath", mockedDataSet);
        doReturn("testPath").when(storage1).getTablePath(S3_PATH, ref, table.getAs("operation"));
        service.when(() -> SourceReferenceService
                        .getSourceReference(table.getAs("source"), table.getAs("table")))
                .thenReturn(Optional.of(ref));
        RawZone rawZoneTest = spy(new RawZone(jobParameters, storage1));
        when(mockedDataSet.count()).thenReturn(10L);
        doReturn(mockedDataSet).when(rawZoneTest).extractRawDataFrame(mockedDataSet, table.getAs("source"),
                table.getAs("table"));
        Dataset<Row> actual_result = rawZoneTest.process(spark, mockedDataSet, table);
        assertNotNull(actual_result);
    }
}
