package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.datareconciliation.model.PrimaryKeyReconciliationCount;
import uk.gov.justice.digital.service.datareconciliation.model.PrimaryKeyReconciliationCounts;

import java.util.List;
import java.util.function.Supplier;

@Singleton
public class PrimaryKeyReconciliationService {

    private static final Logger logger = LoggerFactory.getLogger(PrimaryKeyReconciliationService.class);

    private final S3DataProvider s3DataProvider;
    private final ReconciliationDataSourceService reconciliationDataSourceService;

    @Inject
    public PrimaryKeyReconciliationService(
            S3DataProvider s3DataProvider,
            ReconciliationDataSourceService reconciliationDataSourceService
    ) {
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
        Dataset<Row> curatedPks = s3DataProvider.getPrimaryKeysInCurated(sparkSession, sourceReference);
        logger.debug("Curated schema: {}", (Supplier<String>) () -> curatedPks.schema().treeString());
        Dataset<Row> dataSourcePks = reconciliationDataSourceService.primaryKeysAsDataframe(sparkSession, sourceReference);
        logger.debug("Data Source schema: {}", (Supplier<String>) () -> dataSourcePks.schema().treeString());

        // We cannot use Dataset#exceptAll method with the version of Spark Glue 4.0 provides so
        // we use join instead. See https://issues.apache.org/jira/browse/SPARK-39612
        Column joinExpr = getJoinExpr(sourceReference.getPrimaryKey(), curatedPks, dataSourcePks);
        Dataset<Row> inCuratedNotDataSource = curatedPks.join(dataSourcePks, joinExpr, "left_anti");
        Dataset<Row> inDataSourceNotCurated = dataSourcePks.join(curatedPks, joinExpr, "left_anti");

        long countInCuratedNotDataSource = inCuratedNotDataSource.count();
        if (countInCuratedNotDataSource > 0) {
            logger.error("In Curated not Data Source: {}", (Supplier<String>) () -> inCuratedNotDataSource.showString(20, 0, false));
        }

        long countInDataSourceNotCurated = inDataSourceNotCurated.count();
        if (countInDataSourceNotCurated > 0) {
            logger.error("In Data Source not Curated: {}", (Supplier<String>) () -> inDataSourceNotCurated.showString(20, 0, false));
        }

        return new PrimaryKeyReconciliationCount(countInCuratedNotDataSource, countInDataSourceNotCurated);
    }

    private Column getJoinExpr(SourceReference.PrimaryKey pk, Dataset<Row> left, Dataset<Row> right) {
        return pk.getKeyColumnNames()
                .stream()
                .map(colName -> left.col(colName).equalTo(right.col(colName)))
                .reduce(Column::and)
                .orElseThrow(() -> new IllegalArgumentException("Unable to find join expression for " + left + " and " + right));
    }
}
