package uk.gov.justice.digital.zone;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.util.SourceReference;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

@Singleton
public class RawZone implements Zone {

    private static String DELTA_FORMAT = "delta";
    private String rawPath;

    @Inject
    public RawZone(JobParameters jobParameters){
        this.rawPath = jobParameters.getRawPath();
    }

    @Override
    public Dataset<Row> process(JavaRDD<Row> rowRDD, SparkSession spark) {
        System.out.println("RawZone process started..");
        if(!rowRDD.isEmpty()) {

            StructType schema = new StructType()
                    .add("data", DataTypes.StringType);


            Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
            Dataset<Row> df2 = df.withColumn("jsonData", col("data").cast("string"))
                    .withColumn("data", get_json_object(col("jsonData"), "$.data"))
                    .withColumn("metadata", get_json_object(col("jsonData"), "$.metadata"))
                    .withColumn("source", lower(get_json_object(col("metadata"), "$.schema-name")))
                    .withColumn("table",  lower(get_json_object(col("metadata"), "$.table-name")))
                    .withColumn("operation",  lower(get_json_object(col("metadata"), "$.operation")))
                    .drop("jsonData");


            List<Row> df_tables = df2.filter(col("operation").isin("load"))
                    .select("table", "source", "operation")
                    .distinct().collectAsList();


            for(final Row r : df_tables){
                String table = r.getAs("table").toString().toLowerCase();
                // Internal Source name mapping
                String source = SourceReference.getInternalSource(r.getAs("source")).toLowerCase();
                String operation = r.getAs("operation").toString().toLowerCase();

                System.out.println("Before writing data to S3 raw bucket..");
                // By Delta lake partition
                df2.drop("source", "table", "operation")
                        .filter(col("table").isin(table))
                        .write()
                        .mode(SaveMode.Append)
                        .option("path", getTablePath(rawPath,source,table,operation))
                        .format(DELTA_FORMAT)
                        .save();
            }
            return df2;
        }
        return null;
    }
}
