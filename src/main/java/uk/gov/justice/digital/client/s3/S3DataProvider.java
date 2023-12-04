package uk.gov.justice.digital.client.s3;

import com.google.common.annotations.VisibleForTesting;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.SchemaMismatchException;

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

    public Dataset<Row> getStreamingSourceData(SparkSession sparkSession, SourceReference sourceReference) throws SchemaMismatchException {
        String sourceName = sourceReference.getSource();
        String tableName = sourceReference.getTable();
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);

        String fileGlobPath = ensureEndsWithSlash(tablePath) + arguments.getCdcFileGlobPattern();
        StructType inferredSchema = sparkSession.read().parquet(fileGlobPath).schema();
        StructType schema = withMetadataFields(sourceReference.getSchema());
        logger.info("Provided schema for {}.{}: \n{}", sourceName, tableName, schema.treeString());
        logger.info("Inferred streaming source schema for {}.{}: \n{}", sourceName, tableName, inferredSchema.treeString());
        logger.info("Initialising S3 data source for {}.{} with file glob path {}", sourceName, tableName, fileGlobPath);
        if(schemasMatch(inferredSchema, schema)) {
            return sparkSession
                    .readStream()
                    .schema(schema)
                    .parquet(fileGlobPath);
        } else {
            String msg = "Provided and inferred schemas do not match";
            logger.warn(msg);
            throw new SchemaMismatchException(msg);
        }
    }

    public Dataset<Row> getStreamingSourceDataWithSchemaInference(SparkSession sparkSession, String sourceName, String tableName) {
        String tablePath = tablePath(arguments.getRawS3Path(), sourceName, tableName);

        String fileGlobPath = ensureEndsWithSlash(tablePath) + arguments.getCdcFileGlobPattern();
        StructType schema = sparkSession.read().parquet(fileGlobPath).schema();
        logger.info("Inferred schema for {}.{}: \n{}", sourceName, tableName, schema.treeString());
        logger.info("Initialising S3 data source for {}.{} with file glob path {}", sourceName, tableName, fileGlobPath);
        return sparkSession
                .readStream()
                .schema(schema)
                .parquet(fileGlobPath);
    }

    public Dataset<Row> getBatchSourceData(SparkSession sparkSession, SourceReference sourceReference, List<String> filePaths) throws SchemaMismatchException {
        val scalaFilePaths = JavaConverters.asScalaIteratorConverter(filePaths.iterator()).asScala().toSeq();
        StructType inferredSchema = sparkSession.read().parquet(scalaFilePaths).schema();
        StructType schema = withMetadataFields(sourceReference.getSchema());
        logger.info("Provided schema for {}.{}: \n{}", sourceReference.getSource(), sourceReference.getTable(), schema.treeString());
        logger.info("Inferred batch source schema for {}.{}: \n{}", sourceReference.getSource(), sourceReference.getTable(), inferredSchema.treeString());
        if(schemasMatch(inferredSchema, schema)) {
            return sparkSession
                    .read()
                    .schema(schema)
                    .parquet(scalaFilePaths);
        } else {
            String msg = "Provided and inferred schemas do not match";
            logger.warn(msg);
            throw new SchemaMismatchException(msg);
        }
    }

    public Dataset<Row> getBatchSourceDataWithSchemaInference(SparkSession sparkSession, List<String> filePaths) {
        val scalaFilePaths = JavaConverters.asScalaIteratorConverter(filePaths.iterator()).asScala().toSeq();
        return sparkSession
                .read()
                .parquet(scalaFilePaths);
    }

    @VisibleForTesting
    static boolean schemasMatch(StructType inferredSchema, StructType specifiedSchema) {
        if(inferredSchema.fields().length != specifiedSchema.fields().length) {
            return false;
        }
        for (StructField inferredField : inferredSchema.fields()) {
            try {
                StructField specifiedField = specifiedSchema.apply(inferredField.name());
                DataType inferredDataType = inferredField.dataType();
                DataType specifiedDataType = specifiedField.dataType();
                boolean sameType = specifiedDataType.getClass().equals(inferredDataType.getClass());
                // We represent shorts as ints in avro so this difference is allowed
                boolean allowedDifference = inferredDataType instanceof ShortType && specifiedDataType instanceof IntegerType;
                if(!sameType&& !allowedDifference) {
                    return false;
                }
                // If it is a struct then recurse to check the nested types
                if(inferredDataType instanceof StructType &&
                        !schemasMatch((StructType) inferredDataType, (StructType) specifiedDataType)) {
                    // The struct schemas don't recursively match so the overall schema doesn't match
                    return false;
                }
            } catch (IllegalArgumentException e) {
                // No corresponding field with that name
                return false;
            }
        }
        return true;
    }
}
