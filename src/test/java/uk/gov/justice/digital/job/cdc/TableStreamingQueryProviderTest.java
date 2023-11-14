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

    private TableStreamingQueryProvider underTest;

    @BeforeEach
    public void setUp() {
        underTest = new TableStreamingQueryProvider(arguments, s3DataProvider, batchProcessor, sourceReferenceService);
    }

    @Test
    public void shouldCreateATableStreamingQuery() {
        String source = "some-source";
        String table = "some-table";

        when(sourceReferenceService.getSourceReferenceOrThrow(source, table)).thenReturn(sourceReference);

        TableStreamingQuery streamingQuery = underTest.provide(source, table);
        assertNotNull(streamingQuery);
    }

    @Test
    public void shouldPropagateSourceReferenceThrowable() {
        String source = "some-source";
        String table = "some-table";

        when(sourceReferenceService.getSourceReferenceOrThrow(source, table)).thenThrow(new RuntimeException());
        assertThrows(RuntimeException.class, () -> underTest.provide(source, table));
    }

}