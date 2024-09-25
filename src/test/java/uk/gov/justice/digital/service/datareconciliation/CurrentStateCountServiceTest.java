package uk.gov.justice.digital.service.datareconciliation;

import org.apache.spark.sql.AnalysisException;
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
import uk.gov.justice.digital.exception.ReconciliationDataSourceException;
import uk.gov.justice.digital.exception.OperationalDataStoreException;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.operationaldatastore.OperationalDataStoreService;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CurrentStateCountServiceTest {

    @Mock
    private JobArguments jobArguments;
    @Mock
    private S3DataProvider s3DataProvider;
    @Mock
    private ReconciliationDataSourceService reconciliationDataSourceService;
    @Mock
    private OperationalDataStoreService operationalDataStoreService;
    @Mock
    private SourceReference sourceReference1;
    @Mock
    private SourceReference sourceReference2;
    @Mock
    private SparkSession sparkSession;
    @Mock
    private Dataset<Row> structured1;
    @Mock
    private Dataset<Row> structured2;
    @Mock
    private Dataset<Row> curated1;
    @Mock
    private Dataset<Row> curated2;
    @Mock
    private AnalysisException analysisException;

    @InjectMocks
    private CurrentStateCountService underTest;

    @BeforeEach
    void setUp() {
        when(sourceReference1.getSource()).thenReturn("source");
        when(sourceReference1.getTable()).thenReturn("table");
        when(sourceReference1.getFullOperationalDataStoreTableNameWithSchema()).thenReturn("namespace.source_table");

        when(jobArguments.getStructuredS3Path()).thenReturn("s3://structured");
        when(jobArguments.getCuratedS3Path()).thenReturn("s3://curated");

        when(s3DataProvider.getBatchDeltaTableData(sparkSession, "s3://structured/source/table")).thenReturn(structured1);
        when(s3DataProvider.getBatchDeltaTableData(sparkSession, "s3://curated/source/table")).thenReturn(curated1);
    }

    @Test
    void shouldGetCurrentStateCountsForMultipleTables() {
        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(sourceReference1.getFullDatahubTableName()).thenReturn("source.table");

        long oracleCount1 = 1L;
        long structuredCount1 = 1L;
        long curatedCount1 = 1L;
        long odsCount1 = 1L;

        when(reconciliationDataSourceService.getTableRowCount("table")).thenReturn(oracleCount1);
        when(structured1.count()).thenReturn(structuredCount1);
        when(curated1.count()).thenReturn(curatedCount1);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference1)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table")).thenReturn(odsCount1);

        // Mock results for a 2nd table
        when(sourceReference2.getSource()).thenReturn("source");
        when(sourceReference2.getTable()).thenReturn("table2");
        when(sourceReference2.getFullOperationalDataStoreTableNameWithSchema()).thenReturn("namespace.source_table2");
        when(sourceReference2.getFullDatahubTableName()).thenReturn("source.table2");

        when(s3DataProvider.getBatchDeltaTableData(sparkSession, "s3://structured/source/table2")).thenReturn(structured2);
        when(s3DataProvider.getBatchDeltaTableData(sparkSession, "s3://curated/source/table2")).thenReturn(curated2);

        long oracleCount2 = 4L;
        long structuredCount2 = 3L;
        long curatedCount2 = 2L;
        long odsCount2 = 1L;

        when(reconciliationDataSourceService.getTableRowCount("table2")).thenReturn(oracleCount2);
        when(structured2.count()).thenReturn(structuredCount2);
        when(curated2.count()).thenReturn(curatedCount2);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference2)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table2")).thenReturn(odsCount2);

        CurrentStateTotalCounts results =
                (CurrentStateTotalCounts) underTest.currentStateCounts(sparkSession, Arrays.asList(sourceReference1, sourceReference2));

        CurrentStateTableCount expectedTable1Counts = new CurrentStateTableCount(1L, 1L, 1L, 1L);
        CurrentStateTableCount table1Result = results.get("source.table");
        assertEquals(expectedTable1Counts, table1Result);

        CurrentStateTableCount expectedTable2Counts = new CurrentStateTableCount(4L, 3L, 2L, 1L);
        CurrentStateTableCount table2Result = results.get("source.table2");
        assertEquals(expectedTable2Counts, table2Result);
    }

    @Test
    void shouldGetCurrentStateCountForTable() {
        long oracleCount = 4L;
        long structuredCount = 3L;
        long curatedCount = 2L;
        long odsCount = 1L;

        when(reconciliationDataSourceService.getTableRowCount("table")).thenReturn(oracleCount);
        when(structured1.count()).thenReturn(structuredCount);
        when(curated1.count()).thenReturn(curatedCount);

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference1)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table")).thenReturn(odsCount);

        CurrentStateTableCount result = underTest.currentStateCountForTable(sparkSession, sourceReference1);

        assertEquals(oracleCount, result.getDataSourceCount());
        assertEquals(structuredCount, result.getStructuredCount());
        assertEquals(curatedCount, result.getCuratedCount());
        assertEquals(odsCount, result.getOperationalDataStoreCount());
    }

    @Test
    void shouldGetCurrentStateCountForTableWhenOperationalDataStoreIsDisabled() {
        long oracleCount = 4L;
        long structuredCount = 3L;
        long curatedCount = 2L;

        when(reconciliationDataSourceService.getTableRowCount("table")).thenReturn(oracleCount);
        when(structured1.count()).thenReturn(structuredCount);
        when(curated1.count()).thenReturn(curatedCount);

        when(operationalDataStoreService.isEnabled()).thenReturn(false);

        CurrentStateTableCount result = underTest.currentStateCountForTable(sparkSession, sourceReference1);

        assertEquals(oracleCount, result.getDataSourceCount());
        assertEquals(structuredCount, result.getStructuredCount());
        assertEquals(curatedCount, result.getCuratedCount());
        assertNull(result.getOperationalDataStoreCount());
    }

    @Test
    void shouldGetCurrentStateCountForTableWhenOperationalDataStoreDoesNotManageTheTable() {
        long oracleCount = 4L;
        long structuredCount = 3L;
        long curatedCount = 2L;

        when(reconciliationDataSourceService.getTableRowCount("table")).thenReturn(oracleCount);
        when(structured1.count()).thenReturn(structuredCount);
        when(curated1.count()).thenReturn(curatedCount);

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference1)).thenReturn(false);

        CurrentStateTableCount result = underTest.currentStateCountForTable(sparkSession, sourceReference1);

        assertEquals(oracleCount, result.getDataSourceCount());
        assertEquals(structuredCount, result.getStructuredCount());
        assertEquals(curatedCount, result.getCuratedCount());
        assertNull(result.getOperationalDataStoreCount());
    }

    @Test
    void shouldReturnZeroCountWhenNomisThrows() {
        // It may throw because, for example, the table does not exist
        when(reconciliationDataSourceService.getTableRowCount("table")).thenThrow(
                new ReconciliationDataSourceException("Table does not exist")
        );
        when(structured1.count()).thenReturn(1L);
        when(curated1.count()).thenReturn(1L);

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference1)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table")).thenReturn(1L);

        CurrentStateTableCount result = underTest.currentStateCountForTable(sparkSession, sourceReference1);

        assertEquals(0L, result.getDataSourceCount());
        assertEquals(1L, result.getStructuredCount());
        assertEquals(1L, result.getCuratedCount());
        assertEquals(1L, result.getOperationalDataStoreCount());
    }

    @Test
    void shouldReturnZeroCountWhenStructuredTablePathDoesNotExist() {
        when(reconciliationDataSourceService.getTableRowCount("table")).thenReturn(1L);
        when(analysisException.getMessage()).thenReturn("Path does not exist");
        when(structured1.count()).thenAnswer(i -> {
            throw analysisException;
        });
        when(curated1.count()).thenReturn(1L);

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference1)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table")).thenReturn(1L);

        CurrentStateTableCount result = underTest.currentStateCountForTable(sparkSession, sourceReference1);

        assertEquals(1L, result.getDataSourceCount());
        assertEquals(0L, result.getStructuredCount());
        assertEquals(1L, result.getCuratedCount());
        assertEquals(1L, result.getOperationalDataStoreCount());
    }

    @Test
    void shouldReturnZeroCountWhenCuratedTablePathDoesNotExist() {
        when(reconciliationDataSourceService.getTableRowCount("table")).thenReturn(1L);
        when(analysisException.getMessage()).thenReturn("Path does not exist");
        when(structured1.count()).thenReturn(1L);
        when(curated1.count()).thenAnswer(i -> {
            throw analysisException;
        });

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference1)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table")).thenReturn(1L);

        CurrentStateTableCount result = underTest.currentStateCountForTable(sparkSession, sourceReference1);

        assertEquals(1L, result.getDataSourceCount());
        assertEquals(1L, result.getStructuredCount());
        assertEquals(0L, result.getCuratedCount());
        assertEquals(1L, result.getOperationalDataStoreCount());
    }

    @Test
    void shouldReturnZeroCountWhenODSThrows() {
        when(reconciliationDataSourceService.getTableRowCount("table")).thenReturn(1L);
        when(structured1.count()).thenReturn(1L);
        when(curated1.count()).thenReturn(1L);

        when(operationalDataStoreService.isEnabled()).thenReturn(true);
        when(operationalDataStoreService.isOperationalDataStoreManagedTable(sourceReference1)).thenReturn(true);
        when(operationalDataStoreService.getTableRowCount("namespace.source_table")).thenThrow(
                new OperationalDataStoreException("Table does not exist")
        );

        CurrentStateTableCount result = underTest.currentStateCountForTable(sparkSession, sourceReference1);

        assertEquals(1L, result.getDataSourceCount());
        assertEquals(1L, result.getStructuredCount());
        assertEquals(1L, result.getCuratedCount());
        assertEquals(0L, result.getOperationalDataStoreCount());
    }

}