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
import uk.gov.justice.digital.common.CommonDataFields;
import uk.gov.justice.digital.datahub.model.SourceReference;

import java.util.Arrays;
import java.util.List;

import static uk.gov.justice.digital.common.CommonDataFields.OPERATION;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Delete;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Insert;
import static uk.gov.justice.digital.common.CommonDataFields.ShortOperationCode.Update;
import static uk.gov.justice.digital.common.CommonDataFields.TIMESTAMP;

public class MinimalTestData {
    public static final String PRIMARY_KEY_COLUMN = "pk";
    public static final String DATA_COLUMN = "data";

    public static final SourceReference.PrimaryKey PRIMARY_KEY = new SourceReference.PrimaryKey(PRIMARY_KEY_COLUMN);


    public static final StructType TEST_DATA_SCHEMA = new StructType(new StructField[]{
            new StructField(PRIMARY_KEY_COLUMN, DataTypes.IntegerType, true, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, true, Metadata.empty()),
            new StructField(OPERATION, DataTypes.StringType, true, Metadata.empty()),
            new StructField(DATA_COLUMN, DataTypes.StringType, true, Metadata.empty()),
    });

    public static final StructType SCHEMA_WITHOUT_METADATA_FIELDS = new StructType(new StructField[]{
            new StructField(PRIMARY_KEY_COLUMN, DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(DATA_COLUMN, DataTypes.StringType, true, Metadata.empty()),
    });

    public static final StructType TEST_DATA_SCHEMA_NON_NULLABLE_COLUMNS = new StructType(new StructField[]{
            new StructField(PRIMARY_KEY_COLUMN, DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(TIMESTAMP, DataTypes.StringType, false, Metadata.empty()),
            new StructField(OPERATION, DataTypes.StringType, false, Metadata.empty()),
            new StructField(DATA_COLUMN, DataTypes.StringType, true, Metadata.empty()),
    });
    public static Encoder<Row> encoder = RowEncoder.apply(TEST_DATA_SCHEMA);

    public static Dataset<Row> inserts(SparkSession spark) {
        return spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:50:00.123456", Insert, "1"),
                createRow(2, "2023-11-13 10:50:00.123456", Insert, "2"),
                createRow(3, "2023-11-13 10:50:00.123456", Insert, "3")
        ), TEST_DATA_SCHEMA);
    }
    public static Dataset<Row> rowPerPkDfSameTimestamp(SparkSession spark) {
        return spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Insert, "1a"),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a"),
                createRow(3, "2023-11-13 10:49:28.123456", Insert, "3a"),
                createRow(4, "2023-11-13 10:49:28.123456", Update, "4a"),
                createRow(5, "2023-11-13 10:49:28.123456", Delete, "5a")
        ), TEST_DATA_SCHEMA);
    }

    public static Dataset<Row> manyRowsPerPkDfSameTimestamp(SparkSession spark) {
        return spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.000000", Insert, "1a"),
                createRow(1, "2023-11-13 10:49:30.000000", Delete, "1c"),
                createRow(1, "2023-11-13 10:49:29.000000", Update, "1b"),
                createRow(2, "2023-11-13 10:49:28.000000", Insert, "2a"),
                createRow(2, "2023-11-13 10:49:29.000000", Delete, "2b"),
                createRow(2, "2023-11-13 10:49:30.000000", Update, "2c"),
                createRow(3, "2023-11-13 10:49:28.000000", Update, "3a"),
                createRow(3, "2023-11-13 10:49:29.000000", Delete, "3b"),
                createRow(3, "2023-11-13 10:49:30.000000", Insert, "3c")
        ), TEST_DATA_SCHEMA);
    }

    public static List<Row> manyRowsPerPkSameTimestampLatest() {
        return Arrays.asList(
        createRow(1, "2023-11-13 10:49:30.000000", Delete, "1c"),
        createRow(2, "2023-11-13 10:49:30.000000", Update, "2c"),
        createRow(3, "2023-11-13 10:49:30.000000", Insert, "3c")
        );
    }

    public static Dataset<Row> manyRowsPerPkDfSameTimestampToMicroSecondAccuracy(SparkSession spark) {
        return spark.createDataFrame(Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123456", Insert, "1a"),
                createRow(1, "2023-11-13 10:49:28.123457", Update, "1b"),
                createRow(1, "2023-11-13 10:49:28.123458", Delete, "1c"),
                createRow(2, "2023-11-13 10:49:28.123456", Insert, "2a"),
                createRow(2, "2023-11-13 10:49:28.123457", Delete, "2b"),
                createRow(2, "2023-11-13 10:49:28.123458", Update, "2c"),
                createRow(3, "2023-11-13 10:49:28.123456", Update, "3a"),
                createRow(3, "2023-11-13 10:49:28.123457", Delete, "3b"),
                createRow(3, "2023-11-13 10:49:28.123458", Insert, "3c")
        ), TEST_DATA_SCHEMA);
    }

    public static List<Row> manyRowsPerPkSameTimestampToMicroSecondAccuracyLatest() {
        return Arrays.asList(
                createRow(1, "2023-11-13 10:49:28.123458", Delete, "1c"),
                createRow(2, "2023-11-13 10:49:28.123458", Update, "2c"),
                createRow(3, "2023-11-13 10:49:28.123458", Insert, "3c")
        );
    }

     public static Row createRow(Integer pk, String timestamp, CommonDataFields.ShortOperationCode operation, String data) {
         String operationName;
        // For tests we want to allow nulls for special test cases
        if(operation != null) {
            operationName = operation.getName();
        } else {
            operationName = null;
        }
        return RowFactory.create(pk, timestamp, operationName, data);
     }

     private MinimalTestData() {}
}
