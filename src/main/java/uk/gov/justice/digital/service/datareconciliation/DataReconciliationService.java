package uk.gov.justice.digital.service.datareconciliation;

import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.datareconciliation.model.ReconciliationType;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResult;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.service.datareconciliation.model.ReconciliationType.CHANGE_DATA_COUNTS;


/**
 * Entry point for all data reconciliation in the DataHub.
 */
@Singleton
public class DataReconciliationService {

    private static final Logger logger = LoggerFactory.getLogger(DataReconciliationService.class);

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

    public DataReconciliationResult reconcileData(SparkSession sparkSession) {
        String inputDomain = jobArguments.getConfigKey();
        String dmsTaskId = jobArguments.getDmsTaskId();

        logger.info("Reconciling with input domain: {}, DMS Task ID: {}", inputDomain, dmsTaskId);

        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(inputDomain);
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        Set<ReconciliationType> reconciliationsToRun = jobArguments.getReconciliationsToRun();
        List<DataReconciliationResult> results = reconciliationsToRun.stream().map(toRun -> {
            logger.info("Configured to run {}", toRun);
            switch (toRun) {
                case CHANGE_DATA_COUNTS:
                    return changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, dmsTaskId);
                case CURRENT_STATE_COUNTS:
                    return currentStateCountService.currentStateCounts(sparkSession, allSourceReferences);
                default:
                    throw new IllegalStateException("Unexpected reconciliation result: " + toRun);
            }
        }).collect(Collectors.toList());

        return new DataReconciliationResults(results);
    }
}

