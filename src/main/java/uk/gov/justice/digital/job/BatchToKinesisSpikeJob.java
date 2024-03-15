package uk.gov.justice.digital.job;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

public class BatchToKinesisSpikeJob {

    private static final String PRIMARY_KEY_COLUMN_1 = "pk1";
    private static final String PRIMARY_KEY_COLUMN_2 = "pk2";

    private static final Column[] pkColumns = {
            col("data." + PRIMARY_KEY_COLUMN_1),
            col("data." + PRIMARY_KEY_COLUMN_2),
    };
    private static final String DATA_COLUMN_1 = "data1";
    private static final String DATA_COLUMN_2 = "data2";

    private static final String PARTITION_KEY_COLUMN = "partition_key";
    private static final String JSON_PAYLOAD_COLUMN = "json_payload";

    // Will be instantiated again and used on workers
    private static final AmazonKinesis kinesisClient = AmazonKinesisClientBuilder.defaultClient();
    private static final String kinesisStreamName = "spark-to-rising-wave-maxwell-format-1";

    private static final StructType INPUT_SCHEMA = new StructType(new StructField[]{
            new StructField(PRIMARY_KEY_COLUMN_1, DataTypes.StringType, false, Metadata.empty()),
            new StructField(PRIMARY_KEY_COLUMN_2, DataTypes.StringType, false, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, false, Metadata.empty()),
            new StructField(OPERATION, DataTypes.StringType, false, Metadata.empty()),
            new StructField(DATA_COLUMN_1, DataTypes.StringType, true, Metadata.empty()),
            new StructField(DATA_COLUMN_2, DataTypes.StringType, true, Metadata.empty()),
    });

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("BatchToKinesisSpikeJob")
                .master("local[*]")
                .getOrCreate();

//        Dataset<Row> initialInserts = spark.createDataFrame(Arrays.asList(
//                RowFactory.create("x", "x", "2023-11-13 10:50:00.123456", Insert.getName(), "a1", "b1"),
//                RowFactory.create("y", "y", "2023-11-13 10:50:00.123456", Insert.getName(), "a1", "b1")
//        ), INPUT_SCHEMA);
//        transformAndWriteToKinesis(initialInserts);

//        Dataset<Row> updates = spark.createDataFrame(Arrays.asList(
//                RowFactory.create("x", "x", "2023-11-13 10:50:00.123456", Update.getName(), "a1", "b2"),
//                RowFactory.create("y", "y", "2023-11-13 10:50:00.123456", Update.getName(), "a2", "b2")
//        ), INPUT_SCHEMA);
//        transformAndWriteToKinesis(updates);

        Dataset<Row> deletes = spark.createDataFrame(Collections.singletonList(
                RowFactory.create("y", "y", "2023-11-13 10:50:00.123456", Delete.getName(), "a2", "b2")
        ), INPUT_SCHEMA);
        transformAndWriteToKinesis(deletes);

        spark.stop();


    }

    private static void transformAndWriteToKinesis(Dataset<Row> dmsFormat) {
        dmsFormat.printSchema();
        dmsFormat.show(false);

        Dataset<Row> input1M = toMaxwellFormat(dmsFormat);

        input1M.printSchema();
        input1M.show(false);

        Dataset<Row> input1K = prepareForKinesis(input1M, pkColumns);

        input1K.printSchema();
        input1K.show(false);

        writeToKinesis(input1K);
    }

    private static Dataset<Row> toMaxwellFormat(Dataset<Row> dmsFormat) {
        Column[] columns = Arrays
                .stream(dmsFormat.columns())
                // Remove metadata columns
                .filter(c -> !"_timestamp".equals(c))
                .filter(c -> !"Op".equals(c))
                .map(functions::col)
                .toArray(Column[]::new);

        return dmsFormat
                .withColumn("data", struct(columns))
                .withColumn("type",
                        when(col(OPERATION).equalTo(lit(Insert.getName())), lit("insert"))
                                .when(col(OPERATION).equalTo(lit(Update.getName())), lit("update"))
                                .when(col(OPERATION).equalTo(lit(Delete.getName())), lit("delete"))
                )
                .select("data", "type");
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

    private static void writeToKinesis(Dataset<Row> toWrite) {
        toWrite.foreachPartition(batch -> {
            List<PutRecordsRequestEntry> toSend = new ArrayList<>();
            while (batch.hasNext()) {
                Row record = batch.next();
                String partitionKey = record.getAs(PARTITION_KEY_COLUMN);
                String jsonPayload = record.getAs(JSON_PAYLOAD_COLUMN);
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
                    throw new RuntimeException(result.getErrorMessage());
                } else {
                    System.out.println("Wrote to shard: " + result.getShardId() + " with sequence number: " + result.getSequenceNumber());
                }
            });
        });
    }
}
