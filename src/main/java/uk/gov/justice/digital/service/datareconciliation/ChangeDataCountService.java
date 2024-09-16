package uk.gov.justice.digital.service.datareconciliation;

import com.amazonaws.services.databasemigrationservice.model.TableStatistics;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.dms.DmsClient;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableDmsCount;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableRawZoneCount;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTotalCounts;
import uk.gov.justice.digital.service.datareconciliation.model.CountsByTable;

import java.util.List;

import static java.lang.String.format;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class ChangeDataCountService {

    private static final Logger logger = LoggerFactory.getLogger(ChangeDataCountService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;
    private final DmsClient dmsClient;

    @Inject
    public ChangeDataCountService(
            JobArguments jobArguments,
            S3DataProvider s3DataProvider,
            DmsClient dmsClient
    ) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
        this.dmsClient = dmsClient;
    }

    ChangeDataTotalCounts changeDataCounts(SparkSession sparkSession, List<SourceReference> sourceReferences, String dmsTaskId) {
        CountsByTable<ChangeDataTableDmsCount> dmsCounts = dmsChangeDataCounts(dmsTaskId);
        CountsByTable<ChangeDataTableRawZoneCount> rawCounts = rawArchiveChangeDataCounts(sparkSession, sourceReferences);
        return new ChangeDataTotalCounts(rawCounts, dmsCounts);
    }

    private CountsByTable<ChangeDataTableDmsCount> dmsChangeDataCounts(String dmsTaskId) {
        logger.info("Getting DMS counts by operation for DMS Task ID {}", dmsTaskId);
        List<TableStatistics> dmsTableStatistics = dmsClient.getReplicationTaskTableStatistics(dmsTaskId);
        CountsByTable<ChangeDataTableDmsCount> dmsChangeDataCounts = toDmsChangeDataCounts(dmsTableStatistics);
        logger.info("Finished getting DMS counts by operation for DMS Task ID {}", dmsTaskId);
        return dmsChangeDataCounts;
    }

    private static CountsByTable<ChangeDataTableDmsCount> toDmsChangeDataCounts(List<TableStatistics> dmsTableStatistics) {
        CountsByTable<ChangeDataTableDmsCount> totalCounts = new CountsByTable<>();
        dmsTableStatistics.forEach(tableStatistics -> {
            String schemaName = tableStatistics.getSchemaName();
            // TODO: Must support DPS sources
            //    - need to get the 1st part of the table name from SourceReference / contract's service field
            if (!"OMS_OWNER".equals(schemaName)) {
                // Only OMS_OWNER is supported for now
                throw new UnsupportedOperationException("Unsupported table statistics schema: " + schemaName);
            }
            String fullTableName = format("%s.%s", "nomis", tableStatistics.getTableName().toLowerCase());

            ChangeDataTableDmsCount tableResult = convertToChangeDataTableDmsCount(tableStatistics);
            totalCounts.put(fullTableName, tableResult);
        });
        return totalCounts;
    }

    private static ChangeDataTableDmsCount convertToChangeDataTableDmsCount(TableStatistics tableStatistics) {
        Long insertCount = tableStatistics.getInserts();
        Long updateCount = tableStatistics.getUpdates();
        Long deleteCount = tableStatistics.getDeletes();

        Long appliedInsertCount = tableStatistics.getAppliedInserts();
        Long appliedUpdateCount = tableStatistics.getAppliedUpdates();
        Long appliedDeleteCount = tableStatistics.getAppliedDeletes();

        return new ChangeDataTableDmsCount(
                insertCount, updateCount, deleteCount, appliedInsertCount, appliedUpdateCount, appliedDeleteCount
        );
    }

    private CountsByTable<ChangeDataTableRawZoneCount> rawArchiveChangeDataCounts(SparkSession sparkSession, List<SourceReference> sourceReferences) {
        logger.info("Getting raw zone and raw archive counts by operation");
        CountsByTable<ChangeDataTableRawZoneCount> totalCounts = new CountsByTable<>();
        sourceReferences.forEach(sourceReference -> {
            String tableName = sourceReference.getFullDatahubTableName();
            logger.debug("Getting raw zone counts by operation for table {}", tableName);
            ChangeDataTableRawZoneCount rawZoneCount = changeDataCountsForTable(sparkSession, sourceReference, jobArguments.getRawS3Path());
            logger.debug("Getting raw zone archive counts by operation for table {}", tableName);
            ChangeDataTableRawZoneCount rawArchiveCount = changeDataCountsForTable(sparkSession, sourceReference, jobArguments.getRawArchiveS3Path());
            ChangeDataTableRawZoneCount singleTableCount = rawZoneCount.combineCounts(rawArchiveCount);
            totalCounts.put(tableName, singleTableCount);
        });
        logger.info("Finished getting raw zone and raw archive counts by operation");
        return totalCounts;
    }

    private ChangeDataTableRawZoneCount changeDataCountsForTable(SparkSession sparkSession, SourceReference sourceReference, String s3Path) {
        ChangeDataTableRawZoneCount result = new ChangeDataTableRawZoneCount();

        String rawTablePath = tablePath(s3Path, sourceReference.getSource(), sourceReference.getTable());
        try {
            Dataset<Row> raw = s3DataProvider.getBatchSourceData(sparkSession, rawTablePath);
            List<Row> countsByOperation = raw.groupBy(OPERATION).count().collectAsList();
            // Counts default to zero unless we find a count for that operation below
            countsByOperation.forEach(row -> {
                String operation = row.getString(0);
                long count = row.getLong(1);
                if (Insert.getName().equals(operation)) {
                    result.setInsertCount(count);
                } else if (Update.getName().equals(operation)) {
                    result.setUpdateCount(count);
                } else if (Delete.getName().equals(operation)) {
                    result.setDeleteCount(count);
                } else {
                    logger.error("{} is not a known Operation", operation);
                }
            });
        } catch (Exception e) {
            //  We only want to catch AnalysisException, but we can't be more specific than Exception in what we catch
            //  because the Java compiler will complain that AnalysisException isn't declared as thrown due to Scala trickery.
            if (e instanceof AnalysisException && e.getMessage().startsWith("Path does not exist")) {
                logger.warn("Table does not exist at {} so will set counts to zero", rawTablePath, e);
                result.setInsertCount(0L);
                result.setUpdateCount(0L);
                result.setDeleteCount(0L);
            } else {
                throw e;
            }

        }
        return result;
    }
}
