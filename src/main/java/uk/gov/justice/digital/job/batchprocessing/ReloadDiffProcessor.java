package uk.gov.justice.digital.job.batchprocessing;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.service.DataStorageService;

import javax.inject.Singleton;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;
import static org.apache.spark.sql.functions.row_number;
import static uk.gov.justice.digital.common.CommonDataFields.CHECKPOINT_COL;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.*;
import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

@Singleton
public class ReloadDiffProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ReloadDiffProcessor.class);
    private static final String LEFT_ANTI_JOIN_TYPE = "leftanti";
    private static final String RANK_COL = "archive_recency_rank";
    private static final String DATE_TIME_PATTERN = "yyyyMMddHHmmss";
    private final DataStorageService storageService;

    @Inject
    public ReloadDiffProcessor(DataStorageService storageService) {
        this.storageService = storageService;
    }


    public void createDiff(SourceReference sourceReference, String outputBasePath, Dataset<Row> raw, Dataset<Row> archive, Date reloadTime) {
        String formattedStartTime = new SimpleDateFormat(DATE_TIME_PATTERN).format(reloadTime);
        val windowFunction = Window.partitionBy(getKeyColumnsAsSeq(sourceReference)).orderBy(col(CHECKPOINT_COL).desc());

        Dataset<Row> activeArchiveRecords = archive
                .withColumn(RANK_COL, row_number().over(windowFunction))
                .where(col(RANK_COL).eqNullSafe(lit(1)).and(col(OPERATION).notEqual(lit(Delete.getName()))))
                .drop(RANK_COL)
                .persist();

        logger.info("Processing reload diffs to delete");
        Dataset<Row> recordsToDelete = getRecordsToDelete(sourceReference, raw, activeArchiveRecords)
                .withColumn(CHECKPOINT_COL, lit(formattedStartTime));
        storageService.writeParquet(createOutputPath(sourceReference, outputBasePath, "toDelete"), recordsToDelete);

        logger.info("Processing reload diffs to insert");
        Dataset<Row> recordsToInsert = getRecordsToInsert(sourceReference, raw, activeArchiveRecords)
                .withColumn(CHECKPOINT_COL, lit(formattedStartTime));
        storageService.writeParquet(createOutputPath(sourceReference, outputBasePath, "toInsert"), recordsToInsert);

        logger.info("Processing reload diffs to update");
        Dataset<Row> recordsToUpdate = getRecordsToUpdate(sourceReference, raw, activeArchiveRecords, recordsToInsert)
                .withColumn(CHECKPOINT_COL, lit(formattedStartTime));
        storageService.writeParquet(createOutputPath(sourceReference, outputBasePath, "toUpdate"), recordsToUpdate);

        activeArchiveRecords.unpersist();
    }

    private static Dataset<Row> getRecordsToInsert(SourceReference sourceReference, Dataset<Row> raw, Dataset<Row> archive) {
        return raw
                .join(archive, getKeyColumnNamesSeq(sourceReference), LEFT_ANTI_JOIN_TYPE)
                .withColumn(OPERATION, lit(Insert.getName()));
    }

    private static Dataset<Row> getRecordsToDelete(SourceReference sourceReference, Dataset<Row> raw, Dataset<Row> archive) {
        return archive
                .join(raw, getKeyColumnNamesSeq(sourceReference), LEFT_ANTI_JOIN_TYPE)
                .withColumn(OPERATION, lit(Delete.getName()));
    }

    private Dataset<Row> getRecordsToUpdate(SourceReference sourceReference, Dataset<Row> raw, Dataset<Row> archive, Dataset<Row> recordsToInsert) {
        Seq<String> keyColumnNamesSeq = getKeyColumnNamesSeq(sourceReference);
        Column filterExpression = Arrays.stream(sourceReference.getSchema().fieldNames())
                .map(fieldName -> not(col("raw." + fieldName).eqNullSafe(col("archive." + fieldName))))
                .reduce(Column::or)
                .orElseThrow(() -> new IllegalStateException("Failed to create filter expression"));

        return raw.as("raw")
                .join(recordsToInsert.as("toInsert"), keyColumnNamesSeq, LEFT_ANTI_JOIN_TYPE)
                .join(archive.as("archive"), keyColumnNamesSeq)
                .where(filterExpression)
                .select("raw.*")
                .withColumn(OPERATION, lit(Update.getName()));
    }

    private static Seq<String> getKeyColumnNamesSeq(SourceReference sourceReference) {
        val keyColumnNames = sourceReference.getPrimaryKey().getKeyColumnNames();
        return JavaConverters.asScalaIteratorConverter(keyColumnNames.iterator()).asScala().toSeq();
    }

    private static Seq<Column> getKeyColumnsAsSeq(SourceReference sourceReference) {
        val keyColumns = sourceReference.getPrimaryKey().getKeyColumnNames().stream().map(functions::col);
        return JavaConverters.asScalaIteratorConverter(keyColumns.iterator()).asScala().toSeq();
    }

    private static String createOutputPath(SourceReference sourceReference, String outputPath, String folder) {
        return createValidatedPath(outputPath, folder, sourceReference.getSource(), sourceReference.getTable());
    }
}

