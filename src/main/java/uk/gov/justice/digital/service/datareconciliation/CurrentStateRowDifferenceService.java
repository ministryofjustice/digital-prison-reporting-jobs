package uk.gov.justice.digital.service.datareconciliation;

import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTableRowDifferences;
import uk.gov.justice.digital.service.datareconciliation.model.CurrentStateTotalRowDifferences;

import java.util.List;

import static uk.gov.justice.digital.common.CommonDataFields.dropNonCoreColumns;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class CurrentStateRowDifferenceService {

    private static final Logger logger = LoggerFactory.getLogger(CurrentStateRowDifferenceService.class);

    private final JobArguments jobArguments;
    private final S3DataProvider s3DataProvider;
    private final ReconciliationDataSourceService dataSourceService;
    private final SparkSession sparkSession;

    public CurrentStateRowDifferenceService(
            JobArguments jobArguments,
            S3DataProvider s3DataProvider,
            ReconciliationDataSourceService dataSourceService,
            SparkSessionProvider sparkSessionProvider
    ) {
        this.jobArguments = jobArguments;
        this.s3DataProvider = s3DataProvider;
        this.dataSourceService = dataSourceService;
        this.sparkSession = sparkSessionProvider.getConfiguredSparkSession(jobArguments);
    }

    CurrentStateTotalRowDifferences currentStateRowDifferences(List<SourceReference> sourceReferences) {
        CurrentStateTotalRowDifferences currentStateTotalRowDifferences = new CurrentStateTotalRowDifferences();
        sourceReferences.forEach(sourceReference -> {
            CurrentStateTableRowDifferences tableDifferences = differencesForTable(sourceReference);
            currentStateTotalRowDifferences.put(sourceReference.getFullDatahubTableName(), tableDifferences);
        });
        return currentStateTotalRowDifferences;
    }

    private CurrentStateTableRowDifferences differencesForTable(SourceReference sourceReference) {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        String curatedTablePath = tablePath(jobArguments.getCuratedS3Path(), sourceName, tableName);
        logger.info("Getting current state data across data stores for table {}.{}", sourceName, tableName);
        Dataset<Row> sourceDataStoreData = dataSourceService.readTableAsDataframe(sparkSession, tableName);
        Dataset<Row> curatedData = s3DataProvider.getBatchDeltaTableData(sparkSession, curatedTablePath);
        Dataset<Row> inSourceNotCurated = diff(sourceDataStoreData, curatedData);
        Dataset<Row> inCuratedNotSource = diff(curatedData, sourceDataStoreData);

        Row[] inSourceNotCuratedPks = collectPrimaryKeys(sourceReference, inSourceNotCurated);
        Row[] inCuratedNotSourcePks = collectPrimaryKeys(sourceReference, inCuratedNotSource);


        return new CurrentStateTableRowDifferences(inSourceNotCuratedPks, inCuratedNotSourcePks);
    }

    private Row[] collectPrimaryKeys(SourceReference sourceReference, Dataset<Row> inSourceNotCurated) {
        int numberOfPrimaryKeys = jobArguments.getReconciliationNumPrimaryKeysToDisplay();
        return sourceReference
                .getPrimaryKey()
                .withOnlyPrimaryKeyColumns(inSourceNotCurated)
                .limit(numberOfPrimaryKeys)
                .collectAsList().toArray(new Row[0]);
    }

    private Dataset<Row> diff(Dataset<Row> df1, Dataset<Row> df2) {
        return dropNonCoreColumns(df1).exceptAll(dropNonCoreColumns(df2));
    }
}
