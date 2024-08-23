package uk.gov.justice.digital.client.s3;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.DataProviderFailedMergingSchemasException;
import uk.gov.justice.digital.exception.NoSchemaNoDataException;

import java.io.FileNotFoundException;
import java.util.List;
import java.util.Optional;

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

        return getStreamingDataset(sparkSession, fileGlobPath, schema);
    }

    public Dataset<Row> getStreamingSourceDataWithSchemaInference(SparkSession sparkSession, String sourceName, String tableName) throws NoSchemaNoDataException {
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);

        String fileGlobPath = ensureEndsWithSlash(tablePath) + arguments.getCdcFileGlobPattern();
        try {
            StructType schema = sparkSession.read().parquet(fileGlobPath).schema();
            logger.info("Inferred schema for {}.{}: \n{}", sourceName, tableName, schema.treeString());
            logger.info("Initialising S3 data source for {}.{} with file glob path {}", sourceName, tableName, fileGlobPath);

            return getStreamingDataset(sparkSession, fileGlobPath, schema);
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

    public Dataset<Row> getBatchSourceData(SparkSession sparkSession, List<String> filePaths) throws DataProviderFailedMergingSchemasException {
        try {
            val scalaFilePaths = JavaConverters.asScalaIteratorConverter(filePaths.iterator()).asScala().toSeq();
            return sparkSession
                    .read()
                    .option("mergeSchema", "true")
                    .parquet(scalaFilePaths);
        } catch (Exception e) {
            // We can't catch the specific SparkException type we actually want here because scala erases the fact
            // that a checked exception type is being thrown. The SparkException which reports incompatible schemas
            // can arrive either as the main Exception or wrapped in another Exception so we have to check for both.
            String expectedExceptionMessage = "Failed merging schema";
            boolean isSchemaMergeFail = e.getMessage().startsWith(expectedExceptionMessage);
            boolean isWrappedSchemaMergeFail = e.getCause() instanceof SparkException &&
                    e.getCause().getMessage().startsWith(expectedExceptionMessage);

            if(isSchemaMergeFail || isWrappedSchemaMergeFail) {
                throw new DataProviderFailedMergingSchemasException("Failed merging schemas when getting batch source data", e);
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
        // Attempt to infer schema from files in the raw zone.
        // If there is a failure due to FileNotFoundException which occurs when a file gets archived then the schema inference is done using data already in the raw archive
        // When spark structured streaming archiving is enabled, an attempt is made to infer the schema from the processed files folder before falling back to the archive
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);
        try {
            return sparkSession.read().parquet(tablePath).schema();
        } catch (Exception ex) {
            String failureMessage = format("Failed to infer schema from %s", tablePath);
            logger.info(failureMessage);
            if (rootCauseIsFileNotFound(ex)) {
                String archiveTablePath = tablePath(arguments.getRawArchiveS3Path(), sourceName, tableName);
                if (arguments.enableStreamingSourceArchiving()) {
                    return inferSchemaWhenStreamingArchiveIsEnabled(sparkSession, sourceName, tableName, archiveTablePath);
                } else {
                    return sparkSession.read().parquet(archiveTablePath).schema();
                }
            } else {
                throw ex;
            }
        }
    }

    private Dataset<Row> getStreamingDataset(SparkSession sparkSession, String fileGlobPath, StructType schema) {
        DataStreamReader streamReader = sparkSession.readStream()
                .option("maxFilesPerTrigger", arguments.streamingJobMaxFilePerTrigger())
                .schema(schema);

        if (arguments.enableStreamingSourceArchiving()) {
            String processedRawFilesLocation = ensureEndsWithSlash(arguments.getRawS3Path()) + arguments.getProcessedRawFilesPath();
            return streamReader
                    .option("cleanSource", "archive")
                    .option("sourceArchiveDir", processedRawFilesLocation)
                    .parquet(fileGlobPath);
        } else {
            return streamReader.parquet(fileGlobPath);
        }
    }

    private StructType inferSchemaWhenStreamingArchiveIsEnabled(SparkSession sparkSession, String sourceName, String tableName, String archiveTablePath) {
        try {
            String processedFilesPath = ensureEndsWithSlash(arguments.getRawS3Path()) + arguments.getProcessedRawFilesPath();
            String processedTableFilesPath = tablePath(processedFilesPath, sourceName, tableName);
            String fallbackMessage = format("Failed to infer schema from %s. Falling back to %s", processedTableFilesPath, archiveTablePath);
            logger.info(fallbackMessage);
            return sparkSession.read().parquet(processedTableFilesPath).schema();
        } catch (Exception ex) {
            if (rootCauseIsFileNotFound(ex)) {
                return sparkSession.read().parquet(archiveTablePath).schema();
            } else {
                throw ex;
            }
        }
    }

    @NotNull
    private static Boolean rootCauseIsFileNotFound(Exception ex) {
        // The FileNotFoundException is nested as a root cause of a AnalysisException.
        // Where AnalysisException isCausedBy SparkException isCausedBy FileNotFoundException
        val optionalInnerThrowable = Optional.ofNullable(ex.getCause())
                .flatMap(throwable -> Optional.ofNullable(throwable.getCause()));

        return optionalInnerThrowable.isPresent() && optionalInnerThrowable.get() instanceof FileNotFoundException;
    }
}
