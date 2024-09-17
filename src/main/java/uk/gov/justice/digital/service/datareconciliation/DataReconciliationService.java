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
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResults;

import java.util.List;

import static java.lang.String.format;


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

    public DataReconciliationResults reconcileData(SparkSession sparkSession) {
        String inputDomain = jobArguments.getConfigKey();
        String dmsTaskId = jobArguments.getDmsTaskId();

        logger.info("Reconciling with input domain: {}, DMS Task ID: {}", inputDomain, dmsTaskId);

        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(inputDomain);
        List<SourceReference> allSourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        CurrentStateTotalCounts currentStateTotalCounts = currentStateCountService.currentStateCounts(sparkSession, allSourceReferences);
        ChangeDataTotalCounts changeDataTotalCounts = changeDataCountService.changeDataCounts(sparkSession, allSourceReferences, dmsTaskId);
        return new DataReconciliationResults(currentStateTotalCounts, changeDataTotalCounts);
    }
}

