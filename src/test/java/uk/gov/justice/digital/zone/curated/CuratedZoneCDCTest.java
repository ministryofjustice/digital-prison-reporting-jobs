package uk.gov.justice.digital.zone.curated;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.BaseSparkTest;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.ViolationService;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY;

@ExtendWith(MockitoExtension.class)
class CuratedZoneCDCTest extends BaseSparkTest {

    private static final String curatedRootPath = "/curated/path";
    private static final String curatedTablePath = "/curated/path/source/table";

    @Mock
    private JobArguments arguments;
    @Mock
    private ViolationService violationService;
    @Mock
    private DataStorageService storage;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private static Dataset<Row> df;

    private CuratedZoneCDC underTest;

    @BeforeEach
    public void setUp() {
        when(sourceReference.getPrimaryKey()).thenReturn(PRIMARY_KEY);
        when(sourceReference.getSource()).thenReturn("source");
        when(sourceReference.getTable()).thenReturn("table");
        when(df.count()).thenReturn(1L);
        when(arguments.getCuratedS3Path()).thenReturn(curatedRootPath);
        underTest = new CuratedZoneCDC(arguments, violationService, storage);
    }

    @Test
    public void shouldMergeDataIntoTable() throws DataStorageRetriesExhaustedException {
        underTest.process(spark, df, sourceReference);
        verify(storage, times(1)).mergeRecordsCdc(any(), eq(curatedTablePath), any(), eq(PRIMARY_KEY));
    }

    @Test
    public void shouldUpdateDeltaManifest() {
        underTest.process(spark, df, sourceReference);
        verify(storage, times(1)).updateDeltaManifestForTable(any(), eq(curatedTablePath));
    }

    @Test
    public void shouldHandleRetriesExhausted() throws DataStorageRetriesExhaustedException {
        DataStorageRetriesExhaustedException thrown = new DataStorageRetriesExhaustedException(new Exception());
        doThrow(thrown).when(storage).mergeRecordsCdc(any(), any(), any(), any());

        underTest.process(spark, df, sourceReference);

        verify(violationService, times(1)).handleRetriesExhausted(
                any(), any(), eq("source"), eq("table"), eq(thrown), eq(ViolationService.ZoneName.STRUCTURED_CDC)
        );
    }


}