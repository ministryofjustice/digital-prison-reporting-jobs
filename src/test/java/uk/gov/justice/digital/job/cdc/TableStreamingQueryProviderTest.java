package uk.gov.justice.digital.job.cdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TableStreamingQueryProviderTest {

    @Mock
    private JobArguments arguments;
    @Mock
    private S3DataProvider s3DataProvider;
    @Mock
    private CdcBatchProcessor batchProcessor;
    @Mock
    private SourceReferenceService sourceReferenceService;
    @Mock
    private SourceReference sourceReference;
    @Mock
    private ViolationService violationService;

    private TableStreamingQueryProvider underTest;

    @BeforeEach
    public void setUp() {
        underTest = new TableStreamingQueryProvider(arguments, s3DataProvider, batchProcessor, sourceReferenceService, violationService);
    }

    @Test
    public void shouldCreateATableStreamingQueryToProcessDataWhenSourceReferenceExists() {
        String source = "some-source";
        String table = "some-table";

        when(sourceReferenceService.getSourceReference(source, table)).thenReturn(Optional.of(sourceReference));

        TableStreamingQuery streamingQuery = underTest.provide(source, table);
        assertThat(streamingQuery, instanceOf(ProcessingTableStreamingQuery.class));
    }

    @Test
    public void shouldCreateATableStreamingQueryToWriteAllDataToViolationsWhenNoSourceReference() {
        String source = "some-source";
        String table = "some-table";

        when(sourceReferenceService.getSourceReference(source, table)).thenReturn(Optional.empty());
        TableStreamingQuery streamingQuery = underTest.provide(source, table);
        assertThat(streamingQuery, instanceOf(SchemaViolationTableStreamingQuery.class));
    }

}