package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job;
import com.amazonaws.services.glue.util.JsonOptions;
import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import scala.collection.JavaConverters;
import scala.runtime.BoxedUnit;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.converter.dms.DMS_3_4_7;
import uk.gov.justice.digital.job.batchprocessing.S3CdcProcessor;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.Operation.*;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.OPERATION;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ShortOperationCode.cdcShortOperationCodes;

@Singleton
@CommandLine.Command(name = "DataHubCDCJob")
public class DataHubCdcJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubCdcJob.class);

    private final JobArguments arguments;
    private final SparkSessionProvider sparkSessionProvider;
    private final S3CdcProcessor cdcProcessor;
    private final JobProperties properties;
    private final S3DataProvider s3DataProvider;

    @Inject
    public DataHubCdcJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            S3DataProvider s3DataProvider,
            S3CdcProcessor cdcProcessor
    ) {
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.s3DataProvider = s3DataProvider;
        this.cdcProcessor = cdcProcessor;
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubCdcJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        logger.info("Running DataHubCDCJob");

        String jobName = properties.getSparkJobName();
        GlueContext glueContext = sparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
        SparkSession sparkSession = glueContext.getSparkSession();

        logger.info("Initialising Job");
        Job.init(jobName, glueContext, arguments.getConfig());

        logger.info("Initialising data source");
        Dataset<Row> sourceDf = s3DataProvider.getSourceData(glueContext, arguments);

        logger.info("Initialising per batch processing");
        glueContext.forEachBatch(sourceDf, (batch, batchId) -> {
            try {
                val ShortOperationColumnName = "Op";
                val dataFrame = batch
                        .filter(col(ShortOperationColumnName).isin(cdcShortOperationCodes))
                        .withColumn(
                                OPERATION,
                                when(col(ShortOperationColumnName).equalTo(lit(DMS_3_4_7.ShortOperationCode.Insert.getName())), lit(Insert.getName()))
                                        .when(col(ShortOperationColumnName).equalTo(lit(DMS_3_4_7.ShortOperationCode.Update.getName())), lit(Update.getName()))
                                        .when(col(ShortOperationColumnName).equalTo(lit(DMS_3_4_7.ShortOperationCode.Delete.getName())), lit(Delete.getName()))
                        );

                cdcProcessor.processCDC(sparkSession, dataFrame);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    logger.error("Streaming job interrupted", e);
                } else {
                    logger.error("Exception occurred during streaming job", e);
                    System.exit(1);
                }
            }
            // return type is Unit since we must use the Scala API
            return BoxedUnit.UNIT;
        }, createBatchOptions());

        logger.info("Committing Job");
        Job.commit();

    }

    private JsonOptions createBatchOptions() {
        // See https://docs.aws.amazon.com/glue/latest/dg/glue-etl-scala-apis-glue-gluecontext.html#glue-etl-scala-apis-glue-gluecontext-defs-forEachBatch
        Map<String, String> batchProcessingOptions = new HashMap<>();
        batchProcessingOptions.put("checkpointLocation", arguments.getCheckpointLocation());
        batchProcessingOptions.put("batchMaxRetries", Integer.toString(arguments.getBatchMaxRetries()));
        logger.info("Batch Options: {}", batchProcessingOptions);
        return new JsonOptions(JavaConverters.mapAsScalaMap(batchProcessingOptions));
    }
}
