package uk.gov.justice.digital.service.datareconciliation;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.SparkTestBase;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.PrimaryKeyReconciliationCount;
import uk.gov.justice.digital.service.datareconciliation.model.PrimaryKeyReconciliationCounts;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.test.MinimalTestData.PRIMARY_KEY_COLUMN;
import static uk.gov.justice.digital.test.MinimalTestData.TEST_DATA_SCHEMA;
import static uk.gov.justice.digital.test.MinimalTestData.createRow;
import static uk.gov.justice.digital.test.MinimalTestData.inserts;

@ExtendWith(MockitoExtension.class)
class PrimaryKeyReconciliationServiceTest extends SparkTestBase {
    private static final PrimaryKeyReconciliationCount ALL_MATCHING_COUNTS = new PrimaryKeyReconciliationCount(0L, 0L);
    private static final PrimaryKeyReconciliationCount MORE_IN_CURATED_COUNTS = new PrimaryKeyReconciliationCount(1L, 0L);
    private static final PrimaryKeyReconciliationCount MORE_IN_DATASOURCE_COUNTS = new PrimaryKeyReconciliationCount(0L, 1L);

    private static Dataset<Row> df3Rows;
    private static Dataset<Row> df2Rows;

    @Mock
    private S3DataProvider s3DataProvider;
    @Mock
    private ReconciliationDataSourceService reconciliationDataSourceService;
    @Mock
    private SourceReference sourceReference1;
    @Mock
    private SourceReference sourceReference2;
    @Mock
    private SourceReference.PrimaryKey primaryKey1;
    @Mock
    private SourceReference.PrimaryKey primaryKey2;
    @Mock
    private SparkSession sparkSession;

    @InjectMocks
    private PrimaryKeyReconciliationService underTest;

    @BeforeAll
    static void setUp() {
        df3Rows = spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:50:00.123456", Insert, "1"),
                createRow(2, "2023-11-13 10:50:00.123456", Insert, "2"),
                createRow(3, "2023-11-13 10:50:00.123456", Insert, "3")
        ), TEST_DATA_SCHEMA);

        df2Rows = spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:50:00.123456", Insert, "1"),
                createRow(2, "2023-11-13 10:50:00.123456", Insert, "2")
        ), TEST_DATA_SCHEMA);
    }

    @Test
    void whenAllCountsMatch() {
        Dataset<Row> df = inserts(spark);

        when(sourceReference1.getFullDatahubTableName()).thenReturn("source.table1");
        when(sourceReference1.getPrimaryKey()).thenReturn(primaryKey1);
        when(primaryKey1.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference1)).thenReturn(df);
        when(reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference1)).thenReturn(df);


        PrimaryKeyReconciliationCounts result =
                underTest.primaryKeyReconciliation(sparkSession, Collections.singletonList(sourceReference1));

        PrimaryKeyReconciliationCounts expected = new PrimaryKeyReconciliationCounts();
        expected.put("source.table1", ALL_MATCHING_COUNTS);
        assertEquals(expected, result);
    }

    @Test
    void whenMoreRowsInCurated() {
        Dataset<Row> curated = df3Rows;
        Dataset<Row> dataSource = df2Rows;

        when(sourceReference1.getFullDatahubTableName()).thenReturn("source.table1");
        when(sourceReference1.getPrimaryKey()).thenReturn(primaryKey1);
        when(primaryKey1.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference1)).thenReturn(curated);
        when(reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference1)).thenReturn(dataSource);


        PrimaryKeyReconciliationCounts result =
                underTest.primaryKeyReconciliation(sparkSession, Collections.singletonList(sourceReference1));

        PrimaryKeyReconciliationCounts expected = new PrimaryKeyReconciliationCounts();
        expected.put("source.table1", MORE_IN_CURATED_COUNTS);
        assertEquals(expected, result);
    }

    @Test
    void whenMoreRowsInDataSource() {
        Dataset<Row> curated = df2Rows;
        Dataset<Row> dataSource = df3Rows;

        when(sourceReference1.getFullDatahubTableName()).thenReturn("source.table1");
        when(sourceReference1.getPrimaryKey()).thenReturn(primaryKey1);
        when(primaryKey1.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference1)).thenReturn(curated);
        when(reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference1)).thenReturn(dataSource);


        PrimaryKeyReconciliationCounts result =
                underTest.primaryKeyReconciliation(sparkSession, Collections.singletonList(sourceReference1));

        PrimaryKeyReconciliationCounts expected = new PrimaryKeyReconciliationCounts();
        expected.put("source.table1", MORE_IN_DATASOURCE_COUNTS);
        assertEquals(expected, result);
    }

    @Test
    void whenProcessingMultipleTables() {
        Dataset<Row> curated1 = df3Rows;
        Dataset<Row> dataSource1 = df2Rows;

        Dataset<Row> curated2 = df2Rows;
        Dataset<Row> dataSource2 = df3Rows;

        when(sourceReference1.getFullDatahubTableName()).thenReturn("source.table1");
        when(sourceReference1.getPrimaryKey()).thenReturn(primaryKey1);
        when(primaryKey1.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference1)).thenReturn(curated1);
        when(reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference1)).thenReturn(dataSource1);

        when(sourceReference2.getFullDatahubTableName()).thenReturn("source.table2");
        when(sourceReference2.getPrimaryKey()).thenReturn(primaryKey2);
        when(primaryKey2.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference2)).thenReturn(curated2);
        when(reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference2)).thenReturn(dataSource2);


        PrimaryKeyReconciliationCounts result =
                underTest.primaryKeyReconciliation(sparkSession, Arrays.asList(sourceReference1, sourceReference2));

        PrimaryKeyReconciliationCounts expected = new PrimaryKeyReconciliationCounts();
        expected.put("source.table1", MORE_IN_CURATED_COUNTS);
        expected.put("source.table2", MORE_IN_DATASOURCE_COUNTS);
        assertEquals(expected, result);
    }

    @Test
    void shouldDelegateGettingPrimaryKeys() {
        Dataset<Row> curated1 = df3Rows;
        Dataset<Row> dataSource1 = df2Rows;

        Dataset<Row> curated2 = df2Rows;
        Dataset<Row> dataSource2 = df3Rows;

        when(sourceReference1.getFullDatahubTableName()).thenReturn("source.table1");
        when(sourceReference1.getPrimaryKey()).thenReturn(primaryKey1);
        when(primaryKey1.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference1)).thenReturn(curated1);
        when(reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference1)).thenReturn(dataSource1);

        when(sourceReference2.getFullDatahubTableName()).thenReturn("source.table2");
        when(sourceReference2.getPrimaryKey()).thenReturn(primaryKey2);
        when(primaryKey2.getKeyColumnNames()).thenReturn(Collections.singletonList(PRIMARY_KEY_COLUMN));
        when(s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference2)).thenReturn(curated2);
        when(reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference2)).thenReturn(dataSource2);

        underTest.primaryKeyReconciliation(sparkSession, Arrays.asList(sourceReference1, sourceReference2));

        verify(s3DataProvider, times(1)).getPrimaryKeysInCurated(sparkSession, sourceReference1);
        verify(s3DataProvider, times(1)).getPrimaryKeysInCurated(sparkSession, sourceReference2);
        verify(reconciliationDataSourceService, times(1)).primaryKeysAsDataframe(sparkSession, sourceReference1);
        verify(reconciliationDataSourceService, times(1)).primaryKeysAsDataframe(sparkSession, sourceReference2);
    }
}
