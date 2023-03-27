package uk.gov.justice.digital.converter.dms;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.converter.Converter;

import javax.inject.Named;
import javax.inject.Singleton;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.job.model.Columns.*;

@Singleton
@Named("converterForDMS_3_4_6")
public class DMS_3_4_6 implements Converter {

    private static final StructType eventsSchema =
        new StructType()
            .add(DATA, DataTypes.StringType);

    @Override
    public Dataset<Row> convert(JavaRDD<Row> rdd, SparkSession spark) {
        return spark
            .createDataFrame(rdd, eventsSchema)
            .withColumn(JSON_DATA, col(DATA))
            .withColumn(DATA, get_json_object(col(JSON_DATA), "$.data"))
            .withColumn(METADATA, get_json_object(col(JSON_DATA), "$.metadata"))
            .withColumn(SOURCE, lower(get_json_object(col(METADATA), "$.schema-name")))
            .withColumn(TABLE, lower(get_json_object(col(METADATA), "$.table-name")))
            .withColumn(OPERATION, lower(get_json_object(col(METADATA), "$.operation")))
            .drop(JSON_DATA);
    }

}
