package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Seq;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.PrimaryKeyReconciliationCount;
import uk.gov.justice.digital.service.datareconciliation.model.PrimaryKeyReconciliationCounts;

import java.util.Collection;
import java.util.List;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class PrimaryKeyReconciliationService {

    private static final Logger logger = LoggerFactory.getLogger(PrimaryKeyReconciliationService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;
    private final ReconciliationDataSourceService reconciliationDataSourceService;

    @Inject
    public PrimaryKeyReconciliationService(
            JobArguments jobArguments,
            S3DataProvider s3DataProvider,
            ReconciliationDataSourceService reconciliationDataSourceService
    ) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
        this.reconciliationDataSourceService = reconciliationDataSourceService;
    }

    PrimaryKeyReconciliationCounts primaryKeyReconciliation(SparkSession sparkSession, List<SourceReference> sourceReferences) {
        logger.info("Diffing Curated zone and Data Source primary keys");
        PrimaryKeyReconciliationCounts results = new PrimaryKeyReconciliationCounts();
        sourceReferences.forEach(sourceReference -> {
            logger.info("Diffing Curated zone and Data Source primary keys for {}", sourceReference.getFullDatahubTableName());
            PrimaryKeyReconciliationCount tableCount = primaryKeyReconciliationCountsPerTable(sparkSession, sourceReference);
            results.put(sourceReference.getFullDatahubTableName(), tableCount);
        });
        return results;
    }

    private PrimaryKeyReconciliationCount primaryKeyReconciliationCountsPerTable(SparkSession sparkSession, SourceReference sourceReference) {
        Dataset<Row> curatedPks = primaryKeysInCurated(sparkSession, sourceReference);
        logger.debug("Curated schema: {}", curatedPks.schema().treeString());
        Dataset<Row> dataSourcePks = reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference);
        logger.debug("Data Source schema: {}", dataSourcePks.schema().treeString());

        // We cannot use Dataset#exceptAll method with the version of Spark Glue 4.0 provides so
        // we use join instead. See https://issues.apache.org/jira/browse/SPARK-39612
        Column joinExpr = sourceReference.getPrimaryKey().getJoinExpr(curatedPks, dataSourcePks);
        Dataset<Row> inCuratedNotDataSource = curatedPks.join(dataSourcePks, joinExpr, "left_anti");
        Dataset<Row> inDataSourceNotCurated = dataSourcePks.join(curatedPks, joinExpr, "left_anti");

        long countInCuratedNotDataSource = inCuratedNotDataSource.count();
        if (countInCuratedNotDataSource > 0) {
            logger.error("In Curated not Data Source: {}", inCuratedNotDataSource.showString(20, 0, false));
        }

        long countInDataSourceNotCurated = inDataSourceNotCurated.count();
        if (countInDataSourceNotCurated > 0) {
            logger.error("In Data Source not Curated: {}", inDataSourceNotCurated.showString(20, 0, false));
        }

        return new PrimaryKeyReconciliationCount(countInCuratedNotDataSource, countInDataSourceNotCurated);
    }


    private Dataset<Row> primaryKeysInCurated(SparkSession sparkSession, SourceReference sourceReference) {
        logger.debug("Getting Curated Zone primary keys");
        String fullCuratedPath = tablePath(jobArguments.getCuratedS3Path(), sourceReference.getSource(), sourceReference.getTable());
        Dataset<Row> curated = s3DataProvider.getBatchSourceData(sparkSession, fullCuratedPath);
        Seq<Column> sparkKeyColumns = sourceReference.getPrimaryKey().getSparkKeyColumns();
        return curated.select(sparkKeyColumns);
    }
}
