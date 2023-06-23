package uk.gov.justice.digital.zone;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.KEY;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.zone.fixtures.Fixtures.*;
import static uk.gov.justice.digital.zone.fixtures.ZoneFixtures.createStructuredLoadDataset;

@ExtendWith(MockitoExtension.class)
class CuratedZoneLoadTest extends BaseSparkTest {

    @Mock
    private JobArguments mockJobArguments;

    @Mock
    private SourceReference mockSourceReference;

    @Mock
    private DataStorageService mockDataStorage;

    @Mock
    private SourceReferenceService mockSourceReferenceService;

    @Captor
    ArgumentCaptor<Dataset<Row>> dataframeCaptor;

    private final Dataset<Row> testDataSet = createStructuredLoadDataset(spark);

    String curatedPath = createValidatedPath(CURATED_PATH, TABLE_SOURCE, TABLE_NAME);

    private CuratedZone underTest;


    @BeforeEach
    public void setUp() {
        reset(mockDataStorage);
        when(mockJobArguments.getCuratedS3Path()).thenReturn(CURATED_PATH);

        underTest = new CuratedZoneLoad(
                mockJobArguments,
                mockDataStorage,
                mockSourceReferenceService
        );
    }

    @Test
    public void shouldWriteStructuredRecordsToDeltaTable() throws DataStorageException {
        givenTheSchemaExists();
        givenTheSourceReferenceIsValid();
        doNothing()
                .when(mockDataStorage)
                .appendDistinct(eq(curatedPath), dataframeCaptor.capture(), any());

        assertIterableEquals(
                testDataSet.collectAsList(),
                underTest.process(spark, testDataSet, dataMigrationEventRow).collectAsList()
        );

        assertIterableEquals(
                testDataSet.drop(OPERATION).collectAsList(),
                dataframeCaptor.getValue().collectAsList()
        );
    }

    private void givenTheSchemaExists() {
        when(mockSourceReferenceService.getSourceReference(TABLE_SOURCE, TABLE_NAME))
                .thenReturn(Optional.of(mockSourceReference));
    }

    private void givenTheSourceReferenceIsValid() {
        when(mockSourceReference.getSource()).thenReturn(TABLE_SOURCE);
        when(mockSourceReference.getTable()).thenReturn(TABLE_NAME);
        when(mockSourceReference.getPrimaryKey()).thenReturn(new SourceReference.PrimaryKey(KEY));
    }

}