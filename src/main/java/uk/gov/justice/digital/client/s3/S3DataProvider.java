package uk.gov.justice.digital.client.s3;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.SparkException;
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
import uk.gov.justice.digital.exception.FailedMergingSchemas;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;

import java.util.List;

import static java.lang.String.format;
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
            //  We only want to catch AnalysisException, but we can't be more specific than Exception in what we catch
            //  because the Java compiler will complain that AnalysisException isn't declared as thrown due to Scala trickery.
            if (e instanceof AnalysisException && e.getMessage().startsWith("Path does not exist")) {
                String msg = format("No data available to read and no schema provided to read it with, so we can't run a streaming job for %s.%s", sourceName, tableName);
                logger.error(msg, e);
                throw new NoSchemaNoDataException(msg, e);
            } else {
                // We don't want to handle this here, so rethrow
                throw e;
            }
        }
    }

    public Dataset<Row> getBatchSourceData(SparkSession sparkSession, List<String> filePaths) throws FailedMergingSchemas {
        try {
            val scalaFilePaths = JavaConverters.asScalaIteratorConverter(filePaths.iterator()).asScala().toSeq();
            return sparkSession
                    .read()
                    .option("mergeSchema", "true")
                    .parquet(scalaFilePaths);
        } catch (Exception e) {
            // We can't catch the specific SparkException type we actually want here because scala erases the fact
            // that a checked exception type is being thrown and java seems to optimise away the catch block if it
            // thinks the SparkException can't be thrown in the try block.
            if(e.getMessage().startsWith("Failed merging schema")) {
                throw new FailedMergingSchemas("Failed merging schemas when getting batch source data", e);
            } else {
                throw e;
            }
        }
    }

    public Dataset<Row> getBatchSourceData(SparkSession sparkSession, String filePath) {
        return sparkSession
                .read()
                .option("mergeSchema", "true")
                .parquet(filePath);
    }

    public StructType inferSchema(SparkSession sparkSession, String sourceName, String tableName) {
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);
        return sparkSession.read().parquet(tablePath).schema();
    }
}
