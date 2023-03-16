package uk.gov.justice.digital.zone;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.service.SourceReferenceService;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.get_json_object;
import static org.apache.spark.sql.functions.lower;

@Singleton
public class RawZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    private final String DELTA_FORMAT = "delta";
    private final String rawPath;

    @Inject
    public RawZone(JobParameters jobParameters){
        this.rawPath = jobParameters.getRawPath();
    }

    @Override
    public void process(Dataset<Row> df) {
        logger.info("RawZone process started..");

        List<Row> df_tables = df.filter(col("operation").isin("load"))
                .select("table", "source", "operation")
                .distinct().collectAsList();

        Dataset<Row> df1 = df.drop("source", "table", "operation");

        for(final Row r : df_tables){
            String table = r.getAs("table");
            String source = r.getAs("source");
            String operation = r.getAs("operation");

            final String sourceName = SourceReferenceService.getSource(source +"." + table);
            final String tableName = SourceReferenceService.getTable(source +"." + table);

            logger.info("Before writing data to S3 raw bucket..");
            // By Delta lake partition
            df1.filter(lower(get_json_object(col("metadata"), "$.schema-name")).isin(sourceName)
                            .and(lower(get_json_object(col("metadata"), "$.table-name"))).isin(table))
                    .write()
                    .mode(SaveMode.Append)
                    .option("path", getTablePath(rawPath, sourceName, tableName, operation))
                    .format(DELTA_FORMAT)
                    .save();
        }
    }
}
