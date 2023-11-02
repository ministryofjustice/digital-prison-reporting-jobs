package uk.gov.justice.digital.writer.structured;

import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.writer.Writer;

import javax.inject.Inject;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.last;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;

public class StructuredZoneCdcWriter extends Writer {

    private static final Logger logger = LoggerFactory.getLogger(StructuredZoneCdcWriter.class);

    private final DataStorageService storage;

    @Inject
    public StructuredZoneCdcWriter(DataStorageService storage) {
        this.storage = storage;
    }

    @Override
    public Dataset<Row> writeValidRecords(
            SparkSession spark,
            String destinationPath,
            SourceReference.PrimaryKey primaryKey,
            Dataset<Row> validRecords
    ) throws DataStorageRetriesExhaustedException {
        val mostRecentUniqueRecords = getMostRecentUniqueRecords(spark, primaryKey, validRecords);
        writeCdcRecords(spark, storage, destinationPath, primaryKey, mostRecentUniqueRecords);
        return mostRecentUniqueRecords;
    }

    @Override
    public void writeInvalidRecords(
            SparkSession spark,
            String destinationPath,
            Dataset<Row> invalidRecords
    ) throws DataStorageException {
        logger.info("Appending {} records to deltalake table: {}", invalidRecords.count(), destinationPath);
        storage.append(destinationPath, invalidRecords.drop(OPERATION));

        logger.info("Append completed successfully");
        storage.updateDeltaManifestForTable(spark, destinationPath);
    }

    private static Dataset<Row> getMostRecentUniqueRecords(SparkSession spark, SourceReference.PrimaryKey primaryKey, Dataset<Row> validRecords) {
        val primaryKeys = JavaConverters
                .asScalaIteratorConverter(primaryKey.keys.stream().map(functions::col).iterator())
                .asScala()
                .toSeq();

        val mostRecentColumnExpressions = Arrays.stream(validRecords.columns())
                .filter(column -> !primaryKey.keys.contains(column))
                .map(column -> last(column).as(column));

        val mostRecentColumnExpressionsAsScalaSeq = JavaConverters
                .asScalaIteratorConverter(mostRecentColumnExpressions.iterator())
                .asScala()
                .toSeq();

        if (mostRecentColumnExpressionsAsScalaSeq.nonEmpty()) {
            return validRecords
                    .orderBy(TIMESTAMP)
                    .groupBy(primaryKeys)
                    .agg(mostRecentColumnExpressionsAsScalaSeq.head(), mostRecentColumnExpressionsAsScalaSeq.tail().toSeq())
                    .orderBy(TIMESTAMP);
        } else {
            logger.warn("Unable to get most recent records: Columns {} is empty", mostRecentColumnExpressionsAsScalaSeq.mkString(", "));
            return spark.createDataFrame(Collections.emptyList(), validRecords.schema());
        }
    }

}
