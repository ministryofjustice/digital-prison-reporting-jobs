package uk.gov.justice.digital.job.cdc;

import jakarta.inject.Inject;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Singleton;

@Singleton
public class TableStreamingQueryProvider {

    private final JobArguments arguments;
    private final S3DataProvider s3DataProvider;
    private final CdcBatchProcessor batchProcessor;
    private final SourceReferenceService sourceReferenceService;

    @Inject
    public TableStreamingQueryProvider(
            JobArguments arguments,
            S3DataProvider s3DataProvider,
            CdcBatchProcessor batchProcessor,
            SourceReferenceService sourceReferenceService) {
        this.arguments = arguments;
        this.s3DataProvider = s3DataProvider;
        this.batchProcessor = batchProcessor;
        this.sourceReferenceService = sourceReferenceService;
    }

    public TableStreamingQuery provide(String inputSchemaName, String inputTableName) {
        SourceReference sourceReference = sourceReferenceService.getSourceReferenceOrThrow(inputSchemaName, inputTableName);
        return new TableStreamingQuery(arguments, s3DataProvider, batchProcessor, sourceReference);
    }
}
