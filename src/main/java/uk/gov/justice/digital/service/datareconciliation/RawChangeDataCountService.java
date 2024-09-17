package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.ChangeDataTableCount;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class RawChangeDataCountService {

    private static final Logger logger = LoggerFactory.getLogger(RawChangeDataCountService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;

    @Inject
    public RawChangeDataCountService(JobArguments jobArguments, S3DataProvider s3DataProvider) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
    }

    Map<String, ChangeDataTableCount> changeDataCounts(SparkSession sparkSession, List<SourceReference> sourceReferences) {
        logger.info("Getting raw zone and raw archive counts by operation");
        Map<String, ChangeDataTableCount> totalCounts = new HashMap<>();
        sourceReferences.forEach(sourceReference -> {
            String tableName = sourceReference.getFullDatahubTableName();
            logger.debug("Getting raw zone counts by operation for table {}", tableName);
            ChangeDataTableCount rawZoneCount = changeDataCountsForTable(sparkSession, sourceReference, jobArguments.getRawS3Path());
            logger.debug("Getting raw zone archive counts by operation for table {}", tableName);
            ChangeDataTableCount rawArchiveCount = changeDataCountsForTable(sparkSession, sourceReference, jobArguments.getRawArchiveS3Path());
            ChangeDataTableCount combinedTableCount = rawZoneCount.combineCounts(rawArchiveCount);
            totalCounts.put(tableName, combinedTableCount);
        });
        logger.info("Finished getting raw zone and raw archive counts by operation");
        return totalCounts;
    }

    private ChangeDataTableCount changeDataCountsForTable(SparkSession sparkSession, SourceReference sourceReference, String s3Path) {
        ChangeDataTableCount result = new ChangeDataTableCount();

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
