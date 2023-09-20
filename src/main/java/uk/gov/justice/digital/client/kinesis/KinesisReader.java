package uk.gov.justice.digital.client.kinesis;

import com.amazonaws.services.glue.DataSource;
import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job$;
import com.amazonaws.services.glue.util.JsonOptions;
import io.micronaut.context.annotation.Bean;
import jakarta.inject.Inject;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.runtime.BoxedUnit;
import uk.gov.justice.digital.config.JobArguments;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_6.RECORD_SCHEMA;

@Bean
public class KinesisReader {

    private static final Logger logger = LoggerFactory.getLogger(KinesisReader.class);

    private final JobArguments jobArguments;
    private final GlueContext glueContext;
    private final Job$ job;
    private final DataSource source;

    @Inject
    public KinesisReader(
            JobArguments jobArguments,
            String jobName,
            SparkContext sparkContext
    ) {
        this.jobArguments = jobArguments;
        this.glueContext = new GlueContext(sparkContext);
        this.job = Job$.MODULE$.init(jobName, glueContext, jobArguments.getConfig());

        Map<String, String> kinesisConnectionOptions = new HashMap<>();
        kinesisConnectionOptions.put("streamARN", "arn:aws:kinesis:eu-west-2:771283872747:stream/dpr-kinesis-ingestor-development");
        kinesisConnectionOptions.put("startingPosition", "TRIM_HORIZON");
        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-json-home.html
        kinesisConnectionOptions.put("classification", "json");
        kinesisConnectionOptions.put("inferSchema", "false");
        kinesisConnectionOptions.put("schema", RECORD_SCHEMA.toDDL());
        JsonOptions connectionOptions = new JsonOptions(JavaConverters.mapAsScalaMap(kinesisConnectionOptions));
        this.source = glueContext.getSource("kinesis", connectionOptions, "", "");
    }

    public void runGlueJob(VoidFunction<Dataset<Row>> batchProcessor) {
        // https://docs.aws.amazon.com/glue/latest/dg/glue-etl-scala-apis-glue-gluecontext.html#glue-etl-scala-apis-glue-gluecontext-defs-forEachBatch
        Map<String, String> batchProcessingOptions = new HashMap<>();
        batchProcessingOptions.put("windowSize", jobArguments.getKinesisReaderBatchDuration().toString());
        batchProcessingOptions.put("checkpointLocation", "s3://dpr-working-development/checkpoints");
        batchProcessingOptions.put("batchMaxRetries", "3");
        JsonOptions batchOptions = new JsonOptions(JavaConverters.mapAsScalaMap(batchProcessingOptions));

        glueContext.forEachBatch(source.getDataFrame(), (batch, batchId) -> {
            try {
                batchProcessor.call(batch);
            } catch (Exception e) {
                logger.error("Caught unexpected exception", e);
                throw new RuntimeException("Caught unexpected exception", e);
            }
            return BoxedUnit.UNIT;
        }, batchOptions);
        job.commit();
    }

}
