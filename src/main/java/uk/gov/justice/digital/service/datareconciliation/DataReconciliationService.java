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
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.ReconciliationCheck;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResult;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


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
    private final ReconciliationMetricReportingService metricReportingService;

    @Inject
    public DataReconciliationService(
            JobArguments jobArguments,
            ConfigService configService,
            SourceReferenceService sourceReferenceService,
            CurrentStateCountService currentStateCountService,
            ChangeDataCountService changeDataCountService,
            ReconciliationMetricReportingService metricReportingService
    ) {
        this.jobArguments = jobArguments;
        this.configService = configService;
        this.sourceReferenceService = sourceReferenceService;
        this.currentStateCountService = currentStateCountService;
        this.changeDataCountService = changeDataCountService;
        this.metricReportingService = metricReportingService;
    }

    public DataReconciliationResult reconcileData(SparkSession sparkSession) {
        String inputDomain = jobArguments.getConfigKey();
        String dmsTaskId = jobArguments.getDmsTaskId();

        logger.info("Reconciling with input domain: {}, DMS Task ID: {}", inputDomain, dmsTaskId);

        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(inputDomain);
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        Set<ReconciliationCheck> reconciliationChecksToRun = jobArguments.getReconciliationChecksToRun();
        List<DataReconciliationResult> results = reconciliationChecksToRun.stream().map(checkToRun -> {
            logger.info("Configured to run {}", checkToRun);
            switch (checkToRun) {
                case CHANGE_DATA_COUNTS:
                    ChangeDataTotalCounts changeDataTotalCounts = changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, dmsTaskId);
                    return changeDataTotalCounts;
                case CURRENT_STATE_COUNTS:
                    CurrentStateTotalCounts currentStateTotalCounts = currentStateCountService.currentStateCounts(sparkSession, allSourceReferences);
                    return currentStateTotalCounts;
                default:
                    throw new IllegalStateException("Unexpected reconciliation result: " + checkToRun);
            }
        }).collect(Collectors.toList());

        DataReconciliationResults dataReconciliationResults = new DataReconciliationResults(results);
        metricReportingService.reportMetrics(dataReconciliationResults);
        return dataReconciliationResults;
    }
}

