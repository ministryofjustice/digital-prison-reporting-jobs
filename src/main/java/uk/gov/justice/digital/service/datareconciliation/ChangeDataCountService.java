package uk.gov.justice.digital.service.datareconciliation;

import com.amazonaws.services.databasemigrationservice.model.TableStatistics;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
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
        CountsByTable<ChangeDataTableRawZoneCount> rawCounts = rawZoneChangeDataCounts(sparkSession, sourceReferences);
        return new ChangeDataTotalCounts(rawCounts, dmsCounts);
    }

    private CountsByTable<ChangeDataTableDmsCount> dmsChangeDataCounts(String dmsTaskId) {
        logger.info("Getting DMS counts by operation for DMS Task ID {}", dmsTaskId);
        List<TableStatistics> dmsTableStatistics = dmsClient.getReplicationTaskTableStatistics(dmsTaskId);
        return toDmsChangeDataCounts(dmsTableStatistics);
    }

    private static CountsByTable<ChangeDataTableDmsCount> toDmsChangeDataCounts(List<TableStatistics> dmsTableStatistics) {
        CountsByTable<ChangeDataTableDmsCount> totalCounts = new CountsByTable<>();
        dmsTableStatistics.forEach(tableStatistics -> {
            String fullTableName = format("%s/%s", tableStatistics.getSchemaName(), tableStatistics.getTableName());

            Long insertCount = tableStatistics.getInserts();
            Long updateCount = tableStatistics.getUpdates();
            Long deleteCount = tableStatistics.getDeletes();

            Long appliedInsertCount = tableStatistics.getAppliedInserts();
            Long appliedUpdateCount = tableStatistics.getAppliedUpdates();
            Long appliedDeleteCount = tableStatistics.getAppliedDeletes();

            ChangeDataTableDmsCount tableResult = new ChangeDataTableDmsCount(
                    insertCount, updateCount, deleteCount, appliedInsertCount, appliedUpdateCount, appliedDeleteCount
            );
            totalCounts.put(fullTableName, tableResult);
        });
        return totalCounts;
    }

    private CountsByTable<ChangeDataTableRawZoneCount> rawZoneChangeDataCounts(SparkSession sparkSession, List<SourceReference> sourceReferences) {
        CountsByTable<ChangeDataTableRawZoneCount> totalCounts = new CountsByTable<>();
        sourceReferences.forEach(sourceReference -> {
            String tableName = sourceReference.getFullDatahubTableName();
            logger.info("Getting raw zone counts by operation for table {}", tableName);
            ChangeDataTableRawZoneCount singleTableCount = rawZoneCountForTable(sparkSession, sourceReference);
            totalCounts.put(tableName, singleTableCount);
        });
        return totalCounts;
    }

    private ChangeDataTableRawZoneCount rawZoneCountForTable(SparkSession sparkSession, SourceReference sourceReference) {
        ChangeDataTableRawZoneCount result = new ChangeDataTableRawZoneCount();
        String rawPath = jobArguments.getRawS3Path();

        String rawTablePath = tablePath(rawPath, sourceReference.getSource(), sourceReference.getTable());
        Dataset<Row> raw = s3DataProvider.getBatchSourceData(sparkSession, rawTablePath);
        List<Row> countsByOperation = raw.groupBy(OPERATION).count().collectAsList();
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
        return result;
    }
}
