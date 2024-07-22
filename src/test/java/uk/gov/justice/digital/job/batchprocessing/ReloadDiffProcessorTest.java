package uk.gov.justice.digital.job.batchprocessing;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
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
import uk.gov.justice.digital.service.DataStorageService;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.lit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.CHECKPOINT_COL;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.*;
import static uk.gov.justice.digital.test.MinimalTestData.*;
import static uk.gov.justice.digital.test.SparkTestHelpers.containsTheSameElementsInOrderAs;

@ExtendWith(MockitoExtension.class)
class ReloadDiffProcessorTest extends BaseSparkTest {

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
    private static final String LOAD_CHECKPOINT_VALUE = "";
    private static final Date reloadTime = Date.from(Instant.now());
    private static final String formattedReloadTime = new SimpleDateFormat("yyyyMMddHHmmss", Locale.getDefault()).format(reloadTime);

    private ReloadDiffProcessor underTest;

    @BeforeEach
    public void setUp() {
        reset(dataStorageService, sourceReference);
        underTest = new ReloadDiffProcessor(dataStorageService);
    }

    @Test
    void createReloadDiffShouldProduceEmptyDatasetWhenRawAndArchiveDataAreIdentical() {
        Dataset<Row> rawDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "3", LOAD_CHECKPOINT_VALUE)
        ), TEST_DATA_SCHEMA);

        mockSourceReferenceCall();

        underTest.createDiff(sourceReference, OUTPUT_BASE_PATH, rawDataset, rawDataset, reloadTime);

        verify(dataStorageService, times(3)).writeParquet(outputPathCaptor.capture(), datasetCaptor.capture());
        assertThat(datasetCaptor.getValue().collectAsList(), is(empty()));

        List<String> expectedOutputPaths = Arrays.asList(createPath("toInsert"), createPath("toDelete"), createPath("toUpdate"));
        assertThat(outputPathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedOutputPaths));
    }

    @Test
    void createReloadDiffShouldCreateInsertDatasetWhenThereIsNoArchiveData() {
        Dataset<Row> rawDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "3", LOAD_CHECKPOINT_VALUE)
        ), TEST_DATA_SCHEMA);

        Dataset<Row> expectedDatasetToInsert = rawDataset.withColumn(CHECKPOINT_COL, lit(formattedReloadTime));

        mockSourceReferenceCall();

        underTest.createDiff(sourceReference, OUTPUT_BASE_PATH, rawDataset, spark.emptyDataset(encoder), reloadTime);

        verify(dataStorageService, times(3)).writeParquet(outputPathCaptor.capture(), datasetCaptor.capture());

        List<Dataset<Row>> capturedRecords = datasetCaptor.getAllValues();
        Dataset<Row> recordsToInsert = capturedRecords.get(0);
        Dataset<Row> recordsToDelete = capturedRecords.get(1);
        Dataset<Row> recordsToUpdate = capturedRecords.get(2);

        assertThat(recordsToInsert.collectAsList(), containsInAnyOrder(expectedDatasetToInsert.collectAsList().toArray()));
        assertThat(recordsToDelete.collectAsList(), is(empty()));
        assertThat(recordsToUpdate.collectAsList(), is(empty()));

        List<String> expectedOutputPaths = Arrays.asList(createPath("toInsert"), createPath("toDelete"), createPath("toUpdate"));
        assertThat(outputPathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedOutputPaths));
    }

    @Test
    void createReloadDiffShouldCreateDeleteDatasetWhenArchiveContainsRecordsNotInRaw() {
        Dataset<Row> rawDataset = spark.createDataFrame(Collections.singletonList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", LOAD_CHECKPOINT_VALUE)
        ), TEST_DATA_SCHEMA);

        Dataset<Row> archiveDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", "20240708"),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", "20240708"),
                RowFactory.create(2, "2023-11-14 10:50:00.123456", Update.getName(), "2a", "20240709"), // the most recent version of pk 2 is an update and will therefore be used
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "3", "20240708")
        ), TEST_DATA_SCHEMA);

        List<Row> expectedDatasetToDelete = Arrays.asList(
                RowFactory.create(2, "2023-11-14 10:50:00.123456", Delete.getName(), "2a", formattedReloadTime),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Delete.getName(), "3", formattedReloadTime)
        );

        mockSourceReferenceCall();

        underTest.createDiff(sourceReference, OUTPUT_BASE_PATH, rawDataset, archiveDataset, reloadTime);

        verify(dataStorageService, times(3)).writeParquet(outputPathCaptor.capture(), datasetCaptor.capture());

        List<Dataset<Row>> capturedRecords = datasetCaptor.getAllValues();
        Dataset<Row> recordsToInsert = capturedRecords.get(0);
        Dataset<Row> recordsToDelete = capturedRecords.get(1);
        Dataset<Row> recordsToUpdate = capturedRecords.get(2);

        assertThat(recordsToInsert.collectAsList(), is(empty()));
        assertThat(recordsToDelete.collectAsList(), containsInAnyOrder(expectedDatasetToDelete.toArray()));
        assertThat(recordsToUpdate.collectAsList(), is(empty()));

        List<String> expectedOutputPaths = Arrays.asList(createPath("toInsert"), createPath("toDelete"), createPath("toUpdate"));
        assertThat(outputPathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedOutputPaths));
    }

    @Test
    void createReloadDiffShouldCreateInsertDatasetWhenArchiveIsMissingRecordsContainedInRaw() {
        Dataset<Row> rawDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "3", LOAD_CHECKPOINT_VALUE)
        ), TEST_DATA_SCHEMA);

        Dataset<Row> archiveDataset = spark.createDataFrame(Collections.singletonList(
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", "20240708")
        ), TEST_DATA_SCHEMA);

        List<Row> expectedDatasetToInsert = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", formattedReloadTime),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "3", formattedReloadTime)
        );

        mockSourceReferenceCall();

        underTest.createDiff(sourceReference, OUTPUT_BASE_PATH, rawDataset, archiveDataset, reloadTime);

        verify(dataStorageService, times(3)).writeParquet(outputPathCaptor.capture(), datasetCaptor.capture());

        List<Dataset<Row>> capturedRecords = datasetCaptor.getAllValues();
        Dataset<Row> recordsToInsert = capturedRecords.get(0);
        Dataset<Row> recordsToDelete = capturedRecords.get(1);
        Dataset<Row> recordsToUpdate = capturedRecords.get(2);

        assertThat(recordsToInsert.collectAsList(), containsInAnyOrder(expectedDatasetToInsert.toArray()));
        assertThat(recordsToDelete.collectAsList(), is(empty()));
        assertThat(recordsToUpdate.collectAsList(), is(empty()));

        List<String> expectedOutputPaths = Arrays.asList(createPath("toInsert"), createPath("toDelete"), createPath("toUpdate"));
        assertThat(outputPathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedOutputPaths));
    }

    @Test
    void createReloadDiffShouldCreateUpdateDatasetWhenRawHasUpdatedRecords() {
        Dataset<Row> rawDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "4", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "5", LOAD_CHECKPOINT_VALUE),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "6", LOAD_CHECKPOINT_VALUE)
        ), TEST_DATA_SCHEMA);

        Dataset<Row> archiveDataset = spark.createDataFrame(Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Insert.getName(), "1", "20240708"),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Insert.getName(), "2", "20240708"),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Insert.getName(), "3", "20240708"),
                RowFactory.create(4, "2023-11-13 10:50:00.123456", Insert.getName(), "4", "20240708"),
                RowFactory.create(4, "2023-11-14 10:50:00.123456", Update.getName(), "4a", "20240709"),
                RowFactory.create(4, "2023-11-15 10:50:00.123456", Delete.getName(), "4a", "20240710") // the most recent version of pk 4 is a deletion and will therefore be ignored
        ), TEST_DATA_SCHEMA);

        List<Row> expectedDatasetToInsert = Arrays.asList(
                RowFactory.create(1, "2023-11-13 10:50:00.123456", Update.getName(), "4", formattedReloadTime),
                RowFactory.create(2, "2023-11-13 10:50:00.123456", Update.getName(), "5", formattedReloadTime),
                RowFactory.create(3, "2023-11-13 10:50:00.123456", Update.getName(), "6", formattedReloadTime)
        );

        mockSourceReferenceCall();

        underTest.createDiff(sourceReference, OUTPUT_BASE_PATH, rawDataset, archiveDataset, reloadTime);

        verify(dataStorageService, times(3)).writeParquet(outputPathCaptor.capture(), datasetCaptor.capture());

        List<Dataset<Row>> capturedRecords = datasetCaptor.getAllValues();
        Dataset<Row> recordsToInsert = capturedRecords.get(0);
        Dataset<Row> recordsToDelete = capturedRecords.get(1);
        Dataset<Row> recordsToUpdate = capturedRecords.get(2);

        assertThat(recordsToInsert.collectAsList(), is(empty()));
        assertThat(recordsToDelete.collectAsList(), is(empty()));
        assertThat(recordsToUpdate.collectAsList(), containsInAnyOrder(expectedDatasetToInsert.toArray()));

        List<String> expectedOutputPaths = Arrays.asList(createPath("toInsert"), createPath("toDelete"), createPath("toUpdate"));
        assertThat(outputPathCaptor.getAllValues(), containsTheSameElementsInOrderAs(expectedOutputPaths));
    }

    private void mockSourceReferenceCall() {
        when(sourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey(PRIMARY_KEY_COLUMN));
        when(sourceReference.getSchema()).thenReturn(SCHEMA_WITHOUT_METADATA_FIELDS);
        when(sourceReference.getSource()).thenReturn(SOURCE);
        when(sourceReference.getTable()).thenReturn(TABLE);
    }

    @NotNull
    private static String createPath(String operation) {
        return OUTPUT_BASE_PATH + "/" + operation + "/" + SOURCE + "/" + TABLE;
    }
}