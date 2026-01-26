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
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResult;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;
import uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck;
import uk.gov.justice.digital.service.metrics.BufferedMetricReportingService;

import java.util.List;
import java.util.Set;


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
    private final BufferedMetricReportingService bufferedMetricReportingService;
    private final PrimaryKeyReconciliationService primaryKeyReconciliationService;

    @Inject
    public DataReconciliationService(
            JobArguments jobArguments,
            ConfigService configService,
            SourceReferenceService sourceReferenceService,
            CurrentStateCountService currentStateCountService,
            ChangeDataCountService changeDataCountService,
            BufferedMetricReportingService bufferedMetricReportingService,
            PrimaryKeyReconciliationService primaryKeyReconciliationService
    ) {
        this.jobArguments = jobArguments;
        this.configService = configService;
        this.sourceReferenceService = sourceReferenceService;
        this.currentStateCountService = currentStateCountService;
        this.changeDataCountService = changeDataCountService;
        this.bufferedMetricReportingService = bufferedMetricReportingService;
        this.primaryKeyReconciliationService = primaryKeyReconciliationService;
    }

    public DataReconciliationResult reconcileData(SparkSession sparkSession) {
        String inputDomain = jobArguments.getConfigKey();
        logger.info("Reconciling input domain: {}", inputDomain);

        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(inputDomain);
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        Set<ReconciliationCheck> reconciliationChecksToRun = jobArguments.getReconciliationChecksToRun();
        List<DataReconciliationResult> results = reconciliationChecksToRun.stream().map(checkToRun -> {
            logger.info("Configured to run {}", checkToRun);
            switch (checkToRun) {
                case CHANGE_DATA_COUNTS:
                    String dmsTaskId = jobArguments.getDmsTaskId();
                    logger.info("Getting change data counts with DMS Task ID: {}", dmsTaskId);
                    return changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, dmsTaskId);
                case CURRENT_STATE_COUNTS:
                    logger.info("Getting current state counts");
                    return currentStateCountService.currentStateCounts(sparkSession, allSourceReferences);
                case PRIMARY_KEY_RECONCILIATION:
                    logger.info("Running primary key reconciliation");
                    return primaryKeyReconciliationService.primaryKeyReconciliation(sparkSession, allSourceReferences);
                default:
                    throw new IllegalStateException("Unexpected reconciliation check type: " + checkToRun);
            }
        }).toList();

        DataReconciliationResults dataReconciliationResults = new DataReconciliationResults(results);
        bufferedMetricReportingService.bufferDataReconciliationResults(dataReconciliationResults);
        return dataReconciliationResults;
    }
}

