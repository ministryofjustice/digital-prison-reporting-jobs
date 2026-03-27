package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.DataReconciliationResult;
import uk.gov.justice.digital.service.datareconciliation.model.DmsChangeDataCountsPair;

import java.util.List;
import java.util.Map;

/**
 * Retrieves change data counts for all DataHub data stores which contain change data.
 */
@Singleton
public class ChangeDataCountService {
    private static final Logger logger = LoggerFactory.getLogger(ChangeDataCountService.class);
    private final JobArguments jobArguments;
    private final DmsChangeDataCountService dmsChangeDataCountService;
    private final RawChangeDataCountService rawChangeDataCountService;

    @Inject
    public ChangeDataCountService(
            JobArguments jobArguments,
            DmsChangeDataCountService dmsChangeDataCountService,
            RawChangeDataCountService rawChangeDataCountService
    ) {
        this.jobArguments = jobArguments;
        this.dmsChangeDataCountService = dmsChangeDataCountService;
        this.rawChangeDataCountService = rawChangeDataCountService;
    }

    DataReconciliationResult changeDataCounts(SparkSession sparkSession, List<SourceReference> sourceReferences) {
        Map<String, ChangeDataTableCount> rawCounts = rawChangeDataCountService
                .changeDataCounts(sparkSession, sourceReferences);

        String dmsTaskId = jobArguments.getDmsTaskId();
        logger.info("Getting change data counts with DMS Task ID: {}", dmsTaskId);
        DmsChangeDataCountsPair dmsChangeDataCountsPair = dmsChangeDataCountService
                .dmsChangeDataCounts(sourceReferences, dmsTaskId);

        if (jobArguments.isSplitPipeline()) {
            String cdcDmsTaskId = jobArguments.getCdcDmsTaskId();
            logger.info("Getting change data counts with DMS Task ID: {}", cdcDmsTaskId);
            DmsChangeDataCountsPair dmsCdcChangeDataCountsPair = dmsChangeDataCountService
                    .dmsChangeDataCounts(sourceReferences, cdcDmsTaskId);

            dmsChangeDataCountsPair.sum(dmsCdcChangeDataCountsPair);
        }

        return new ChangeDataTotalCounts(
                rawCounts,
                dmsChangeDataCountsPair.getDmsChangeDataCounts(),
                dmsChangeDataCountsPair.getDmsAppliedChangeDataCounts()
        );
    }
}
