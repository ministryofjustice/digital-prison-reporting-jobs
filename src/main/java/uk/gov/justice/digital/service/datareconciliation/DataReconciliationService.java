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
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.List;

import static java.lang.String.format;


/**
 * Entry point for all data reconciliation in the DataHub.
 */
@Singleton
public class DataReconciliationService {

    private final JobArguments jobArguments;
    private final ConfigService configService;
    private final SourceReferenceService sourceReferenceService;
    private final CurrentStateCountService currentStateCountService;
    private final ChangeDataCountService changeDataCountService;

    @Inject
    public DataReconciliationService(
            JobArguments jobArguments,
            ConfigService configService,
            SourceReferenceService sourceReferenceService,
            CurrentStateCountService currentStateCountService,
            ChangeDataCountService changeDataCountService
    ) {
        this.jobArguments = jobArguments;
        this.configService = configService;
        this.sourceReferenceService = sourceReferenceService;
        this.currentStateCountService = currentStateCountService;
        this.changeDataCountService = changeDataCountService;
    }

    public DataReconciliationResults reconcileData(SparkSession sparkSession) {
        String inputDomain = jobArguments.getConfigKey();
        // TODO This needs to work for DPS as well as Nomis so sort out how taskId is configured
        String dmsTaskId = format("dpr-dms-nomis-oracle-s3-%s-task-development", inputDomain);
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(inputDomain);
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        CurrentStateTotalCounts currentStateTotalCounts = currentStateCountService.currentStateCounts(sparkSession, allSourceReferences);
        ChangeDataTotalCounts changeDataTotalCounts = changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, dmsTaskId);
        return new DataReconciliationResults(currentStateTotalCounts, changeDataTotalCounts);
    }
}

