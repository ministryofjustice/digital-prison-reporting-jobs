package uk.gov.justice.digital.client.s3;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;

import java.util.List;

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

    public Dataset<Row> getStreamingSourceData(SparkSession sparkSession, SourceReference sourceReference) {
        String source = sourceReference.getSource();
        String table = sourceReference.getTable();
        String tablePath = tablePath(arguments.getRawS3Path(), source, table);

        String fileGlobPath = ensureEndsWithSlash(tablePath) + arguments.getCdcFileGlobPattern();
        StructType schema = withMetadataFields(sourceReference.getSchema());
        logger.info("Provided schema for {}.{}: \n{}", source, table, schema.treeString());
        logger.info("Initialising S3 data source for {}.{} with file glob path {}", source, table, fileGlobPath);
        return sparkSession
                .readStream()
                .schema(schema)
                .parquet(fileGlobPath);
    }

    public Dataset<Row> getStreamingSourceDataWithSchemaInference(SparkSession sparkSession, String sourceName, String tableName) throws NoSchemaNoDataException {
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);

        String fileGlobPath = ensureEndsWithSlash(tablePath) + arguments.getCdcFileGlobPattern();
        try {
            StructType schema = sparkSession.read().parquet(fileGlobPath).schema();
            logger.info("Inferred schema for {}.{}: \n{}", sourceName, tableName, schema.treeString());
            logger.info("Initialising S3 data source for {}.{} with file glob path {}", sourceName, tableName, fileGlobPath);
            return sparkSession
                    .readStream()
                    .schema(schema)
                    .parquet(fileGlobPath);
        } catch (Exception e) {
            if (e instanceof AnalysisException && e.getMessage().startsWith("Path does not exist")) {
                String msg = "No data available to read and no schema";
                logger.error(msg, e);
                throw new NoSchemaNoDataException(msg, e);
            } else {
                throw e;
            }
        }
    }

    public Dataset<Row> getBatchSourceData(SparkSession sparkSession, SourceReference sourceReference, List<String> filePaths) {
        String source = sourceReference.getSource();
        String table = sourceReference.getTable();
        val scalaFilePaths = JavaConverters.asScalaIteratorConverter(filePaths.iterator()).asScala().toSeq();
        StructType schema = withMetadataFields(sourceReference.getSchema());
        logger.info("Provided schema for {}.{}: \n{}", source, table, schema.treeString());
        return sparkSession
                .read()
                .schema(schema)
                .parquet(scalaFilePaths);
    }

    public Dataset<Row> getBatchSourceDataWithSchemaInference(SparkSession sparkSession, List<String> filePaths) {
        val scalaFilePaths = JavaConverters.asScalaIteratorConverter(filePaths.iterator()).asScala().toSeq();
        return sparkSession
                .read()
                .parquet(scalaFilePaths);
    }

    public StructType inferSchema(SparkSession sparkSession, String sourceName, String tableName) {
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);
        return sparkSession.read().parquet(tablePath).schema();
    }
}
