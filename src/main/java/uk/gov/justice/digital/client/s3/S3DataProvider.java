package uk.gov.justice.digital.client.s3;

import com.amazonaws.services.glue.DataSource;
import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.JsonOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;

import java.util.HashMap;
import java.util.Map;

public class S3DataProvider {

    private static final Logger logger = LoggerFactory.getLogger(S3DataProvider.class);

    public Dataset<Row> getSourceData(GlueContext glueContext, JobArguments arguments) {
        logger.info("Initialising S3 data source");
        Map<String, String> s3ConnectionOptions = new HashMap<>();

        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-json-home.html
        s3ConnectionOptions.put("inferSchema", "true");
        s3ConnectionOptions.put("paths", arguments.getRawS3Path() + "*/*/*-*.parquet");
        logger.info("S3 Connection Options: {}", s3ConnectionOptions);
        JsonOptions connectionOptions = new JsonOptions(JavaConverters.mapAsScalaMap(s3ConnectionOptions));
        DataSource s3DataSource =  glueContext.getSource("s3", connectionOptions, "", "");
        return s3DataSource.getDataFrame();
    }
}
