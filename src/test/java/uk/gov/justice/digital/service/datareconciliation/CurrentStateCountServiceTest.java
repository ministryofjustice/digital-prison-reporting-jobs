package uk.gov.justice.digital.service.datareconciliation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.NomisDataAccessService;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateCountTableResult;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CurrentStateCountServiceTest {

    @Mock
    private JobArguments jobArguments;
    @Mock
    private S3DataProvider s3DataProvider;
    @Mock
    private NomisDataAccessService nomisDataAccessService;
    @Mock
    private OperationalDataStoreService operationalDataStoreService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private SparkSession sparkSession;
    @Mock
    private Dataset<Row> structured;
    @Mock
    private Dataset<Row> curated;

    @InjectMocks
    private CurrentStateCountService underTest;

    @BeforeEach
    void setUp() {
        when(sourceReference.getSource()).thenReturn("source");
        when(sourceReference.getTable()).thenReturn("table");
        when(sourceReference.getFullOperationalDataStoreTableNameWithSchema()).thenReturn("namespace.source_table");

        when(jobArguments.getNomisSourceSchemaName()).thenReturn("OMS_OWNER");
        when(jobArguments.getStructuredS3Path()).thenReturn("s3://structured");
        when(jobArguments.getCuratedS3Path()).thenReturn("s3://curated");

        when(s3DataProvider.getBatchSourceData(sparkSession, "s3://structured/source/table")).thenReturn(structured);
        when(s3DataProvider.getBatchSourceData(sparkSession, "s3://curated/source/table")).thenReturn(curated);
    }

    @Test
    void shouldGetCurrentStateCounts() {
        long oracleCount = 4L;
        long structuredCount = 3L;
        long curatedCount = 2L;
        long odsCount = 1L;

        when(nomisDataAccessService.getTableRowCount("OMS_OWNER.TABLE")).thenReturn(oracleCount);
        when(structured.count()).thenReturn(structuredCount);
        when(curated.count()).thenReturn(curatedCount);

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table")).thenReturn(odsCount);

        CurrentStateCountTableResult result = underTest.currentStateCounts(sparkSession, sourceReference);

        assertEquals(oracleCount, result.getNomisCount());
        assertEquals(structuredCount, result.getStructuredCount());
        assertEquals(curatedCount, result.getCuratedCount());
        assertEquals(odsCount, result.getOperationalDataStoreCount());

    }

    @Test
    void shouldGetCurrentStateCountsWhenOperationalDataStoreIsDisabled() {
        long oracleCount = 4L;
        long structuredCount = 3L;
        long curatedCount = 2L;
        long odsCount = 1L;

        when(nomisDataAccessService.getTableRowCount("OMS_OWNER.TABLE")).thenReturn(oracleCount);
        when(structured.count()).thenReturn(structuredCount);
        when(curated.count()).thenReturn(curatedCount);

        when(operationalDataStoreService.isEnabled()).thenReturn(false);

        CurrentStateCountTableResult result = underTest.currentStateCounts(sparkSession, sourceReference);

        assertEquals(oracleCount, result.getNomisCount());
        assertEquals(structuredCount, result.getStructuredCount());
        assertEquals(curatedCount, result.getCuratedCount());
        assertNull(result.getOperationalDataStoreCount());
    }

    @Test
    void shouldGetCurrentStateCountsWhenOperationalDataStoreDoesNotManageTheTable() {
        long oracleCount = 4L;
        long structuredCount = 3L;
        long curatedCount = 2L;
        long odsCount = 1L;

        when(nomisDataAccessService.getTableRowCount("OMS_OWNER.TABLE")).thenReturn(oracleCount);
        when(structured.count()).thenReturn(structuredCount);
        when(curated.count()).thenReturn(curatedCount);

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference)).thenReturn(false);

        CurrentStateCountTableResult result = underTest.currentStateCounts(sparkSession, sourceReference);

        assertEquals(oracleCount, result.getNomisCount());
        assertEquals(structuredCount, result.getStructuredCount());
        assertEquals(curatedCount, result.getCuratedCount());
        assertNull(result.getOperationalDataStoreCount());
    }

}