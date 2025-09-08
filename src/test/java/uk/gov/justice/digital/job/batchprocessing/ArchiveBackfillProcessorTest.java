package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.BackfillException;
import uk.gov.justice.digital.service.DataStorageService;

import java.util.ArrayList;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verify;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.SCHEMA_WITHOUT_METADATA_FIELDS;

@ExtendWith(MockitoExtension.class)
class ArchiveBackfillProcessorTest extends BaseSparkTest {

    @Mock
    private DataStorageService dataStorageService;
    @Mock
    private SourceReference sourceReference;
    @Captor
    ArgumentCaptor<Dataset<Row>> datasetCaptor;
    @Captor
    ArgumentCaptor<String> outputPathCaptor;

    private static final String SOURCE = "source";
    private static final String TABLE = "table";
    private static final String OUTPUT_BASE_PATH = "s3://bucket/output-folder";
    private static final StructField nullableField = new StructField("new_column", DataTypes.StringType, true, Metadata.empty());
    private static final StructField nonNullableField = new StructField("new_column", DataTypes.StringType, false, Metadata.empty());

    private ArchiveBackfillProcessor underTest;

    @BeforeEach
    void setUp() {
        reset(dataStorageService, sourceReference);
        underTest = new ArchiveBackfillProcessor(dataStorageService);
    }

    @Test
    void shouldHandleAdditionOfNullableColumns() {
        Dataset<Row> archiveDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", "20240709"),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", "20240709")
        ), TEST_DATA_SCHEMA);

        Dataset<Row> expectedDatasetToUpdate = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", "20240709", null),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", "20240709", null)
        ), TEST_DATA_SCHEMA.add(nullableField));

        mockSourceReferenceCallWithNewNullableField();

        underTest.createBackfilledArchiveData(sourceReference, OUTPUT_BASE_PATH, archiveDataset);

        verify(dataStorageService).overwriteParquet(outputPathCaptor.capture(), datasetCaptor.capture());

        Dataset<Row> capturedRecords = datasetCaptor.getValue();
        assertThat(capturedRecords.collectAsList(), containsInAnyOrder(expectedDatasetToUpdate.collectAsList().toArray()));

        assertThat(outputPathCaptor.getValue(), is(equalTo(createPath())));
    }

    @Test
    void shouldFailWhenNonNullableColumnsAreAdded() {
        Dataset<Row> archiveDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", "20240709"),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", "20240709")
        ), TEST_DATA_SCHEMA);

        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS.add(nonNullableField));

        assertThrows(
                BackfillException.class, () ->
                underTest.createBackfilledArchiveData(sourceReference, OUTPUT_BASE_PATH, archiveDataset)
        );
    }

    @Test
    void shouldFailWhenArchiveDataIsEmpty() {
        Dataset<Row> emptyArchiveDataset = spark.createDataFrame(new ArrayList<>(), TEST_DATA_SCHEMA);

        mockSourceReferenceCall();

        assertThrows(BackfillException.class, () -> underTest.createBackfilledArchiveData(sourceReference, OUTPUT_BASE_PATH, emptyArchiveDataset));

        verifyNoInteractions(dataStorageService);
    }

    private void mockSourceReferenceCall() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getSource()).thenReturn(SOURCE);
        when(sourceReference.getTable()).thenReturn(TABLE);
    }

    private void mockSourceReferenceCallWithNewNullableField() {
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS.add(nullableField));
        when(sourceReference.getSource()).thenReturn(SOURCE);
        when(sourceReference.getTable()).thenReturn(TABLE);
    }

    @NotNull
    private static String createPath() {
        return OUTPUT_BASE_PATH + "/" + SOURCE + "/" + TABLE;
    }
}