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

@Singleton
public class RawZone implements Zone {

    private static final Logger logger = LoggerFactory.getLogger(RawZone.class);

    private final String DELTA_FORMAT = "delta";
    private final String LOAD_OPERATION = "load";
    private final String rawPath;

    @Inject
    public RawZone(JobParameters jobParameters){
        this.rawPath = jobParameters
            .getRawS3Path()
            .orElseThrow(() -> new IllegalStateException("raw s3 path not set - unable to create RawZone instance"));
    }

    public String getRawPath() {
        return this.rawPath;
    }

    @Override
    public void process(Dataset<Row> df) {
        logger.info("RawZone process started..");

        List<Row> sourceReferenceData = getSourceReferenceData(df);

        for(final Row row : sourceReferenceData) {

            Dataset<Row> df1 = df;
            String table = getTableName(row);
            String source = getSourceName(row);
            String operation = row.getAs("operation");


            logger.info("Before writing data to S3 raw bucket..");
            // By Delta lake partition
            df1.filter(col("source").isin(source)
                            .and(col("table").isin(table)))
                    .drop("source", "table", "operation")
                    .write()
                    .mode(SaveMode.Append)
                    .option("path", getTablePath(getRawPath(), source, table, operation))
                    .format(DELTA_FORMAT)
                    .save();
        }
    }

    public List<Row> getSourceReferenceData(Dataset<Row> df) {
        return df.filter(col("operation").isin(LOAD_OPERATION))
                .select("table", "source", "operation")
                .distinct().collectAsList();
    }

    public String getSourceName(Row row) {
        String table = row.getAs("table");
        String source = row.getAs("source");
        return SourceReferenceService.getSource(source +"." + table);
    }

    public String getTableName(Row row) {
        String table = row.getAs("table");
        String source = row.getAs("source");
        return SourceReferenceService.getTable(source +"." + table);
    }

}
