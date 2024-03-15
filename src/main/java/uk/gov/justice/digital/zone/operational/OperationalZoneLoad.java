package uk.gov.justice.digital.zone.operational;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.zone.Zone;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;
import static org.apache.spark.sql.functions.when;
import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

@Singleton
public class OperationalZoneLoad implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(OperationalZoneLoad.class);

    private static final AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.defaultClient();

    private static final String PARTITION_KEY_COLUMN = "partition_key";
    private static final String JSON_PAYLOAD_COLUMN = "json_payload";
    private static final String MAXWELL_DATA_COLUMN = "data";
    private static final String MAXWELL_TYPE_COLUMN = "type";
    @Override
    public Dataset<Row> process(SparkSession spark, Dataset<Row> dataFrame, SourceReference sourceReference) {
        val startTime = System.currentTimeMillis();
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        String kinesisStream = kinesisStreamName(sourceName, tableName);
        SourceReference.PrimaryKey primaryKey = sourceReference.getPrimaryKey();
        logger.debug("Processing records for operational zone {}/{} stream: {}", sourceName, tableName, kinesisStream);
        logger.debug("PK columns are {}", primaryKey.getKeyColumnNames());

        Column[] pkColumns = primaryKey
                .getKeyColumnNames()
                .stream()
                .map(pk -> format("%s.%s", MAXWELL_DATA_COLUMN, pk))
                .map(functions::col)
                .toArray(Column[]::new);

        Dataset<Row> maxwellFormatDf = toMaxwellFormat(dataFrame);
        Dataset<Row> kinesisEncodedDf = prepareForKinesis(maxwellFormatDf, pkColumns);

        writeToKinesis(kinesisEncodedDf, kinesisStream);
        logger.info("Processed batch for operational zone {}/{} in {}ms", sourceName, tableName, System.currentTimeMillis() - startTime);
        // Return the original dataframe
        return dataFrame;
    }

    private static Dataset<Row> toMaxwellFormat(Dataset<Row> dmsFormat) {
        Column[] columns = Arrays
                .stream(dmsFormat.columns())
                // Remove metadata columns
                .filter(c -> !TIMESTAMP.equals(c))
                .filter(c -> !OPERATION.equals(c))
                .map(functions::col)
                .toArray(Column[]::new);

        return dmsFormat
                .withColumn(MAXWELL_DATA_COLUMN, struct(columns))
                .withColumn(MAXWELL_TYPE_COLUMN,
                        when(col(OPERATION).equalTo(lit(Insert.getName())), lit("insert"))
                                .when(col(OPERATION).equalTo(lit(Update.getName())), lit("update"))
                                .when(col(OPERATION).equalTo(lit(Delete.getName())), lit("delete"))
                )
                .select(MAXWELL_DATA_COLUMN, MAXWELL_TYPE_COLUMN);
    }

    private static Dataset<Row> prepareForKinesis(Dataset<Row> maxwellFormat, Column[] pkColumns) {
        return maxwellFormat
                .withColumn(
                        JSON_PAYLOAD_COLUMN,
                        to_json(struct(col("*")))
                )
                .withColumn(
                        PARTITION_KEY_COLUMN,
                        concat_ws(",", pkColumns)
                )
                .select(JSON_PAYLOAD_COLUMN, PARTITION_KEY_COLUMN);
    }

    private static String kinesisStreamName(String sourceName, String tableName) {
        return format("dpr-operational-updates-%s-%s", sourceName, tableName);
    }

    private static void writeToKinesis(Dataset<Row> toWrite, String kinesisStreamName) {
        toWrite.foreachPartition(partition -> {
            logger.debug("Writing partition to Kinesis");
            List<PutRecordsRequestEntry> toSend = new ArrayList<>();
            while (partition.hasNext()) {
                Row row = partition.next();
                String partitionKey = row.getAs(PARTITION_KEY_COLUMN);
                String jsonPayload = row.getAs(JSON_PAYLOAD_COLUMN);
                ByteBuffer payload = ByteBuffer.wrap(jsonPayload.getBytes(StandardCharsets.UTF_8));
                toSend.add(
                        new PutRecordsRequestEntry()
                                .withPartitionKey(partitionKey)
                                .withData(payload)
                );
            }
            PutRecordsRequest putRecordsRequest = new PutRecordsRequest()
                    .withStreamName(kinesisStreamName)
                    .withRecords(toSend);
            PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
            putRecordsResult.getRecords().forEach(result -> {
                if (result.getErrorCode() != null) {
                    logger.error("Failed to write to Kinesis: " + result.getErrorCode());
                    throw new RuntimeException(result.getErrorMessage());
                } else {
                    logger.trace("Wrote to shard: " + result.getShardId() + " with sequence number: " + result.getSequenceNumber());
                }
            });
        });
    }
}