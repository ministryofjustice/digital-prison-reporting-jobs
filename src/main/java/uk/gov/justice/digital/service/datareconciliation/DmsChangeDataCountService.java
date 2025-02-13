package uk.gov.justice.digital.service.datareconciliation;

import com.amazonaws.services.databasemigrationservice.model.TableStatistics;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableCount;
import uk.gov.justice.digital.service.datareconciliation.model.DmsChangeDataCountsPair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Retrieves change data counts from the AWS DMS API.
 */
@Singleton
public class DmsChangeDataCountService {

    private static final Logger logger = LoggerFactory.getLogger(DmsChangeDataCountService.class);

    private final JobArguments jobArguments;
    private final DmsClient dmsClient;

    @Inject
    public DmsChangeDataCountService(JobArguments jobArguments, DmsClient dmsClient) {
        this.jobArguments = jobArguments;
        this.dmsClient = dmsClient;
    }

    /**
     * Retrieves the change data counts that the DMS thinks it has read as well as that it thinks it has applied.
     */
    DmsChangeDataCountsPair dmsChangeDataCounts(List<SourceReference> sourceReferences, String dmsTaskId) {
        logger.info("Getting DMS table statistics for DMS Task ID {}", dmsTaskId);
        List<TableStatistics> dmsTableStatistics = dmsClient.getReplicationTaskTableStatistics(dmsTaskId);
        logger.info("Finished getting DMS table statistics for DMS Task ID {}", dmsTaskId);
        Map<String, String> tableToSource = tableToSourceLookup(sourceReferences);

        Map<String, ChangeDataTableCount> dmsChangeDataCounts = new HashMap<>();
        Map<String, ChangeDataTableCount> dmsAppliedChangeDataCounts = new HashMap<>();
        dmsTableStatistics.forEach(tableStatistics -> {
            String tableName = tableStatistics.getTableName().toLowerCase();
            // We can't use the schema on the table statistics because it is the input schema rather than the 'source'.
            // It might be 'OMS_OWNER', for example, when we need 'nomis'.
            String source = tableToSource.get(tableName);
            if (source != null) {
                String fullTableName = format("%s.%s", source, tableName);
                dmsChangeDataCounts.put(fullTableName, convertToChangeDataTableCount(tableStatistics));
                dmsAppliedChangeDataCounts.put(fullTableName, convertToAppliedChangeDataTableCount(tableStatistics));
            } else {
                logger.warn("Cannot find table {} (with source schema {}) in the SourceReferences", tableName, tableStatistics.getSchemaName());
            }
        });
        return new DmsChangeDataCountsPair(dmsChangeDataCounts, dmsAppliedChangeDataCounts);
    }

    private static Map<String, String> tableToSourceLookup(List<SourceReference> sourceReferences) {
        return sourceReferences.stream().collect(Collectors.toMap(SourceReference::getTable, SourceReference::getSource));
    }

    private ChangeDataTableCount convertToChangeDataTableCount(TableStatistics tableStatistics) {
        Long cdcInsertCount = tableStatistics.getInserts();
        Long fullLoadInserts = tableStatistics.getFullLoadRows();
        Long cdcUpdateCount = tableStatistics.getUpdates();
        Long cdcDeleteCount = tableStatistics.getDeletes();

        double relativeTolerance = jobArguments.getReconciliationChangeDataCountsToleranceRelativePercentage();
        long absoluteTolerance = jobArguments.getReconciliationChangeDataCountsToleranceAbsolute();

        return new ChangeDataTableCount(relativeTolerance, absoluteTolerance, cdcInsertCount + fullLoadInserts, cdcUpdateCount, cdcDeleteCount);
    }

    private ChangeDataTableCount convertToAppliedChangeDataTableCount(TableStatistics tableStatistics) {
        Long appliedInsertCount = tableStatistics.getAppliedInserts();
        Long fullLoadInserts = tableStatistics.getFullLoadRows();
        Long appliedCdcUpdateCount = tableStatistics.getAppliedUpdates();
        Long appliedCdcDeleteCount = tableStatistics.getAppliedDeletes();

        double relativeTolerance = jobArguments.getReconciliationChangeDataCountsToleranceRelativePercentage();
        long absoluteTolerance = jobArguments.getReconciliationChangeDataCountsToleranceAbsolute();

        return new ChangeDataTableCount(relativeTolerance, absoluteTolerance, appliedInsertCount + fullLoadInserts, appliedCdcUpdateCount, appliedCdcDeleteCount);
    }
}
