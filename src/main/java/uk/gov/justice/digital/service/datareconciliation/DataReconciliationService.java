package uk.gov.justice.digital.service.datareconciliation;

import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateCountTableResult;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCountResults;

import java.util.List;


/**
 * Entry point for all data reconciliation in the DataHub.
 */
@Singleton
public class DataReconciliationService {

    private final JobArguments jobArguments;
    private final ConfigService configService;
    private final SourceReferenceService sourceReferenceService;
    private final CurrentStateCountService currentStateCountService;

    @Inject
    public DataReconciliationService(
            JobArguments jobArguments,
            ConfigService configService,
            SourceReferenceService sourceReferenceService,
            CurrentStateCountService currentStateCountService
    ) {
        this.jobArguments = jobArguments;
        this.configService = configService;
        this.sourceReferenceService = sourceReferenceService;
        this.currentStateCountService = currentStateCountService;
    }

    public CurrentStateTotalCountResults reconcileData(SparkSession sparkSession) {
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(jobArguments.getConfigKey());
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        CurrentStateTotalCountResults results = new CurrentStateTotalCountResults();
        allSourceReferences.forEach(sourceReference -> {
            CurrentStateCountTableResult countResults = currentStateCountService.currentStateCounts(sparkSession, sourceReference);
            results.put(sourceReference.getFullDatahubTableName(), countResults);
        });
        return results;
    }
}

