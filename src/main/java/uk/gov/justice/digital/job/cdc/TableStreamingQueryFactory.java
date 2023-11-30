package uk.gov.justice.digital.job.cdc;

import jakarta.inject.Inject;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.job.batchprocessing.CdcBatchProcessor;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class TableStreamingQueryFactory {

    private final JobArguments arguments;
    private final S3DataProvider s3DataProvider;
    private final CdcBatchProcessor batchProcessor;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    @Inject
    public TableStreamingQueryFactory(
            JobArguments arguments,
            S3DataProvider s3DataProvider,
            CdcBatchProcessor batchProcessor,
            SourceReferenceService sourceReferenceService, ViolationService violationService) {
        this.arguments = arguments;
        this.s3DataProvider = s3DataProvider;
        this.batchProcessor = batchProcessor;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
    }

    public TableStreamingQuery create(String inputSourceName, String inputTableName) {
        Optional<SourceReference> maybeSourceReference = sourceReferenceService.getSourceReference(inputSourceName, inputTableName);
        if(maybeSourceReference.isPresent()) {
            SourceReference sourceReference = maybeSourceReference.get();
            return new StandardProcessingTableStreamingQuery(arguments, s3DataProvider, batchProcessor, sourceReference);
        } else {
            return new SchemaViolationTableStreamingQuery(inputSourceName, inputTableName, arguments, s3DataProvider, violationService);
        }
    }
}
