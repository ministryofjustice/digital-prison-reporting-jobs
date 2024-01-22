package uk.gov.justice.digital.job;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;
import uk.gov.justice.digital.job.cdc.TableStreamingQuery;
import uk.gov.justice.digital.job.cdc.TableStreamingQueryProvider;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.TableDiscoveryService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataHubCdcJobTest {
    private static final SparkSessionProvider sparkSessionProvider = new SparkSessionProvider();

    private static final List<ImmutablePair<String, String>> tablesToProcess = Arrays.asList(
            new ImmutablePair<>("source1", "table1"),
            new ImmutablePair<>("source2", "table2"),
            new ImmutablePair<>("source3", "table3")
    );

    @Mock
    private JobArguments arguments;
    @Mock
    private JobProperties properties;
    @Mock
    private TableStreamingQueryProvider tableStreamingQueryProvider;
    @Mock
    private TableDiscoveryService tableDiscoveryService;
    @Mock
    private SparkSession spark;
    @Mock
    private TableStreamingQuery table1StreamingQuery;
    @Mock
    private TableStreamingQuery table2StreamingQuery;
    @Mock
    private TableStreamingQuery table3StreamingQuery;

    private DataHubCdcJob underTest;

    @BeforeEach
    public void setUp() {
        underTest = new DataHubCdcJob(arguments, properties, sparkSessionProvider, tableStreamingQueryProvider, tableDiscoveryService);
    }

    @Test
    public void shouldRunAQueryPerTable() {

        when(tableDiscoveryService.discoverTablesToProcess()).thenReturn(tablesToProcess);

        when(tableStreamingQueryProvider.provide(spark, "source1", "table1")).thenReturn(table1StreamingQuery);
        when(tableStreamingQueryProvider.provide(spark, "source2", "table2")).thenReturn(table2StreamingQuery);
        when(tableStreamingQueryProvider.provide(spark, "source3", "table3")).thenReturn(table3StreamingQuery);

        underTest.runJob(spark);

        verify(table1StreamingQuery, times(1)).runQuery();
        verify(table2StreamingQuery, times(1)).runQuery();
        verify(table3StreamingQuery, times(1)).runQuery();
    }

    @Test
    public void shouldSkipQueryWhenThereIsNoSchemaAndNoData() {
        when(tableDiscoveryService.discoverTablesToProcess()).thenReturn(tablesToProcess);

        when(tableStreamingQueryProvider.provide(spark, "source1", "table1")).thenReturn(table1StreamingQuery);
        when(tableStreamingQueryProvider.provide(spark, "source2", "table2"))
                .thenThrow(new NoSchemaNoDataException("", new Exception()));
        when(tableStreamingQueryProvider.provide(spark, "source3", "table3")).thenReturn(table3StreamingQuery);

        underTest.runJob(spark);

        verify(table1StreamingQuery, times(1)).runQuery();
        verify(table2StreamingQuery, times(0)).runQuery();
        verify(table3StreamingQuery, times(1)).runQuery();
    }

    @Test
    public void shouldNotThrowForNoTables() {
        when(tableDiscoveryService.discoverTablesToProcess()).thenReturn(Collections.emptyList());

        underTest.runJob(spark);
    }

}