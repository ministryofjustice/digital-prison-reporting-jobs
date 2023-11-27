package uk.gov.justice.digital.client.s3;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;

import static uk.gov.justice.digital.common.CommonDataFields.withMetadataFields;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;
import static uk.gov.justice.digital.common.ResourcePath.tablePath;

/**
 * Responsible for providing a streaming Dataset of database change files from S3.
 * The files are processed in creation timestamp order.
 */
@Singleton
public class S3DataProvider {

    private final JobArguments arguments;

    private static final Logger logger = LoggerFactory.getLogger(S3DataProvider.class);

    @Inject
    public S3DataProvider(JobArguments arguments) {
        this.arguments = arguments;
    }

    public Dataset<Row> getSourceData(SparkSession sparkSession, SourceReference sourceReference) {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);

        String fileGlobPath = ensureEndsWithSlash(tablePath) + arguments.getCdcFileGlobPattern();
        StructType schema = withMetadataFields(sourceReference.getSchema());
        logger.info("Schema for {}.{}: \n{}", sourceName, tableName, schema.treeString());
        logger.info("Initialising S3 data source for {}.{} with file glob path {}", sourceName, tableName, fileGlobPath);
        return sparkSession
                .readStream()
                .schema(schema)
                .parquet(fileGlobPath);
    }
}
