package uk.gov.justice.digital.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.domain.model.SourceReference;

import java.util.ArrayList;
import java.util.List;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;

public class MinimalTestData {
    public static final String PRIMARY_KEY_COLUMN = "pk";
    public static final String DATA_COLUMN = "data";

    public static final SourceReference.PrimaryKey primaryKey = new SourceReference.PrimaryKey(PRIMARY_KEY_COLUMN);


    public static final StructType TEST_DATA_SCHEMA = new StructType(new StructField[]{
            new StructField("pk", DataTypes.StringType, true, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty()),
            new StructField("Op", DataTypes.StringType, true, Metadata.empty()),
            new StructField("data", DataTypes.StringType, true, Metadata.empty()),
    });

    public static final StructType TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS = new StructType(new StructField[]{
            new StructField("pk", DataTypes.StringType, false, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, false, Metadata.empty()),
            new StructField("Op", DataTypes.StringType, false, Metadata.empty()),
            new StructField("data", DataTypes.StringType, true, Metadata.empty()),
    });

    public static Encoder<Row> encoder = RowEncoder.apply(TEST_DATA_SCHEMA);
    public static Dataset<Row> rowPerPkDfSameTimestamp(SparkSession spark) {
        List<Row> input = new ArrayList<>();
        input.add(RowFactory.create("1", "2023-11-13 10:49:28.123456", "I", "1a"));
        input.add(RowFactory.create("2", "2023-11-13 10:49:28.123456", "I", "2a"));
        input.add(RowFactory.create("3", "2023-11-13 10:49:28.123456", "I", "3a"));
        input.add(RowFactory.create("4", "2023-11-13 10:49:28.123456", "U", "4a"));
        input.add(RowFactory.create("5", "2023-11-13 10:49:28.123456", "D", "5a"));

        return spark.createDataFrame(input, TEST_DATA_SCHEMA);
    }

    public static Dataset<Row> manyRowsPerPkDfSameTimestamp(SparkSession spark) {
        List<Row> input = new ArrayList<>();
        input.add(RowFactory.create("1", "2023-11-13 10:49:28.000000", "I", "1a"));
        input.add(RowFactory.create("1", "2023-11-13 10:49:30.000000", "D", "1c"));
        input.add(RowFactory.create("1", "2023-11-13 10:49:29.000000", "U", "1b"));
        input.add(RowFactory.create("2", "2023-11-13 10:49:28.000000", "I", "2a"));
        input.add(RowFactory.create("2", "2023-11-13 10:49:29.000000", "D", "2b"));
        input.add(RowFactory.create("2", "2023-11-13 10:49:30.000000", "U", "2c"));
        input.add(RowFactory.create("3", "2023-11-13 10:49:28.000000", "U", "3a"));
        input.add(RowFactory.create("3", "2023-11-13 10:49:29.000000", "D", "3b"));
        input.add(RowFactory.create("3", "2023-11-13 10:49:30.000000", "I", "3c"));

        return spark.createDataFrame(input, TEST_DATA_SCHEMA);
    }

    public static List<Row> manyRowsPerPkSameTimestampLatest() {
        List<Row> latestRows = new ArrayList<>();
        latestRows.add(RowFactory.create("1", "2023-11-13 10:49:30.000000", "D", "1c"));
        latestRows.add(RowFactory.create("2", "2023-11-13 10:49:30.000000", "U", "2c"));
        latestRows.add(RowFactory.create("3", "2023-11-13 10:49:30.000000", "I", "3c"));
        return latestRows;
    }

    public static Dataset<Row> manyRowsPerPkDfSameTimestampToMicroSecondAccuracy(SparkSession spark) {
        List<Row> input = new ArrayList<>();
        input.add(RowFactory.create("1", "2023-11-13 10:49:28.123456", "I", "1a"));
        input.add(RowFactory.create("1", "2023-11-13 10:49:28.123457", "U", "1b"));
        input.add(RowFactory.create("1", "2023-11-13 10:49:28.123458", "D", "1c"));
        input.add(RowFactory.create("2", "2023-11-13 10:49:28.123456", "I", "2a"));
        input.add(RowFactory.create("2", "2023-11-13 10:49:28.123457", "D", "2b"));
        input.add(RowFactory.create("2", "2023-11-13 10:49:28.123458", "U", "2c"));
        input.add(RowFactory.create("3", "2023-11-13 10:49:28.123456", "U", "3a"));
        input.add(RowFactory.create("3", "2023-11-13 10:49:28.123457", "D", "3b"));
        input.add(RowFactory.create("3", "2023-11-13 10:49:28.123458", "I", "3c"));

        return spark.createDataFrame(input, TEST_DATA_SCHEMA);
    }

    public static List<Row> manyRowsPerPkSameTimestampToMicroSecondAccuracyLatest() {
        List<Row> latestRows = new ArrayList<>();
        latestRows.add(RowFactory.create("1", "2023-11-13 10:49:28.123458", "D", "1c"));
        latestRows.add(RowFactory.create("2", "2023-11-13 10:49:28.123458", "U", "2c"));
        latestRows.add(RowFactory.create("3", "2023-11-13 10:49:28.123458", "I", "3c"));
        return latestRows;
    }
}
