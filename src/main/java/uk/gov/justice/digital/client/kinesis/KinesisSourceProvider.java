package uk.gov.justice.digital.client.kinesis;

import com.amazonaws.services.glue.DataSource;
import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.JsonOptions;
import io.micronaut.context.annotation.Bean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.RECORD_SCHEMA;

@Bean
public class KinesisSourceProvider {

    private static final Logger logger = LoggerFactory.getLogger(KinesisSourceProvider.class);

    public DataSource getKinesisSource(GlueContext glueContext, JobArguments arguments) {
        logger.info("Initialising Kinesis data source");
        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-connect-kinesis-home.html
        Map<String, String> kinesisConnectionOptions = new HashMap<>();
        kinesisConnectionOptions.put("streamARN", arguments.getKinesisStreamArn());
        kinesisConnectionOptions.put("startingPosition", arguments.getKinesisStartingPosition());
        // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-json-home.html
        kinesisConnectionOptions.put("classification", "json");
        kinesisConnectionOptions.put("inferSchema", "false");
        kinesisConnectionOptions.put("schema", RECORD_SCHEMA.toDDL());
        logger.info("Kinesis Connection Options: {}", kinesisConnectionOptions);
        JsonOptions connectionOptions = new JsonOptions(JavaConverters.mapAsScalaMap(kinesisConnectionOptions));
        return glueContext.getSource("kinesis", connectionOptions, "", "");
    }
}
