package uk.gov.justice.digital.job;

import com.amazonaws.services.glue.GlueContext;
import com.amazonaws.services.glue.util.Job;
import com.google.common.annotations.VisibleForTesting;
import io.micronaut.configuration.picocli.PicocliRunner;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import scala.collection.JavaConverters;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.domain.model.SourceReference;
import uk.gov.justice.digital.exception.DataStorageRetriesExhaustedException;
import uk.gov.justice.digital.job.context.MicronautContext;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.DataStorageService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.ViolationService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;
import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.SOURCE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TABLE;
import static uk.gov.justice.digital.converter.dms.DMS_3_4_7.ParsedDataFields.TIMESTAMP;
import static uk.gov.justice.digital.service.ViolationService.ZoneName.CDC;

@Singleton
@CommandLine.Command(name = "DataHubCdcJob")
public class DataHubCdcJob implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DataHubCdcJob.class);

    private final JobArguments arguments;
    private final JobProperties properties;
    private final SparkSessionProvider sparkSessionProvider;
    private final S3DataProvider s3DataProvider;
    private final SourceReferenceService sourceReferenceService;
    private final ViolationService violationService;

    private final DataStorageService storage;

    @Inject
    public DataHubCdcJob(
            JobArguments arguments,
            JobProperties properties,
            SparkSessionProvider sparkSessionProvider,
            S3DataProvider s3DataProvider,
            SourceReferenceService sourceReferenceService,
            ViolationService violationService,
            DataStorageService storage) {
        logger.info("Initializing DataHubCdcJob");
        this.arguments = arguments;
        this.properties = properties;
        this.sparkSessionProvider = sparkSessionProvider;
        this.s3DataProvider = s3DataProvider;
        this.sourceReferenceService = sourceReferenceService;
        this.violationService = violationService;
        this.storage = storage;
        logger.info("DataHubCdcJob initialization complete");
    }

    public static void main(String[] args) {
        logger.info("Job started");
        PicocliRunner.run(DataHubCdcJob.class, MicronautContext.withArgs(args));
    }

    @Override
    public void run() {
        boolean runLocal = System.getProperty(SPARK_JOB_NAME_PROPERTY) == null;
        if(runLocal) {
            logger.info("Running locally");
            SparkConf sparkConf = new SparkConf().setAppName("DataHubCdcJob local").setMaster("local[*]");
            SparkSession spark = sparkSessionProvider.getConfiguredSparkSession(sparkConf, arguments.getLogLevel());
            runJob(spark);
        } else {
            logger.info("Running in Glue");
            String jobName = properties.getSparkJobName();
            GlueContext glueContext = sparkSessionProvider.createGlueContext(jobName, arguments.getLogLevel());
            SparkSession spark = glueContext.getSparkSession();
            Job.init(jobName, glueContext, arguments.getConfig());
            runJob(spark);
            Job.commit();
        }
    }

    private void runJob(SparkSession spark) {
        logger.info("Initialising Job");
        List<ImmutablePair<String, String>> tablesToProcess = discoverTablesToProcess();

        tablesToProcess.forEach(tableDetails -> {
            String inputSchemaName = tableDetails.getLeft();
            String inputTableName = tableDetails.getRight();
            SourceReference sourceReference = getSourceReference(inputSchemaName, inputTableName);
            processTable(inputSchemaName, inputTableName, sourceReference, spark);
        });
        try {
            spark.streams().awaitAnyTermination();
        } catch (StreamingQueryException e) {
            logger.error("A streaming query terminated with an Exception", e);
            throw new RuntimeException(e);
        }
        logger.info("Job finished");
    }

    public void processTable(String inputSchemaName, String inputTableName, SourceReference sourceReference, SparkSession spark) {
        String outputSourceName = sourceReference.getSource();
        String outputTableName = sourceReference.getTable();
        logger.info("Initialising data source for {}/{}", outputSourceName, outputTableName);
        Dataset<Row> sourceDf = s3DataProvider.getSourceData(spark, arguments, inputSchemaName, inputTableName);
        if(!schemaIsValid(sourceDf.schema(), sourceReference)) {
            logger.error("Schema for {}/{} is not valid\n{}", outputSourceName, outputTableName, sourceDf.schema().treeString());
            throw new RuntimeException(format("Schema for %s.%s is not valid %s", outputSourceName, outputTableName, sourceDf.schema().catalogString()));
        }
        logger.info("Initialising per batch processing for {}/{}", outputSourceName, outputTableName);

        val primaryKey = sourceReference.getPrimaryKey();
        val structuredTablePath = format("%s%s/%s", ensureEndsWithSlash(arguments.getStructuredS3Path()), outputSourceName, outputTableName);
        val curatedTablePath = format("%s%s/%s", ensureEndsWithSlash(arguments.getCuratedS3Path()), outputSourceName, outputTableName);
        try {
            String queryName = format("Datahub CDC %s.%s", inputSchemaName, inputTableName);
            logger.info("Initialising query {}", queryName);
            sourceDf
                    .writeStream()
                    .queryName(queryName)
                    .format("delta")
                    .foreachBatch((df, batchId) -> {
                        logger.info("Processing batch {}", batchId);
                        val batchStartTime = System.currentTimeMillis();
                        val latestCDCRecordsByPK = latestRecords(df.drop(SOURCE, TABLE), primaryKey);
                        try {
                            storage.writeCdc(spark, structuredTablePath, latestCDCRecordsByPK, primaryKey);
                            storage.writeCdc(spark, curatedTablePath, latestCDCRecordsByPK, primaryKey);
                        } catch (DataStorageRetriesExhaustedException e) {
                            violationService.handleRetriesExhausted(spark, sourceDf, sourceReference.getSource(), sourceReference.getTable(), e, CDC);
                        }
                        logger.info("Batch processing for batch {} took {}ms", batchId, System.currentTimeMillis() - batchStartTime);
                    })
                    .outputMode("update")
                    .option("checkpointLocation", ensureEndsWithSlash(arguments.getCheckpointLocation()) + queryName)
                    .start();
            logger.info("Started query {}", queryName);
        } catch (TimeoutException e) {
            logger.error("Encountered TimeoutException when running streaming query start", e);
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static List<ImmutablePair<String, String>> discoverTablesToProcess() {
        // TODO Discover the tables from S3 or fix the SourceReferenceService
        List<ImmutablePair<String, String>> tablesToProcess = new ArrayList<>();
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "AGENCY_INTERNAL_LOCATIONS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "AGENCY_LOCATIONS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "MOVEMENT_REASONS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "OFFENDER_BOOKINGS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "OFFENDER_EXTERNAL_MOVEMENTS"));
        tablesToProcess.add(new ImmutablePair<>("OMS_OWNER", "OFFENDERS"));
        return tablesToProcess;
    }

    @NotNull
    private SourceReference getSourceReference(String inputSchemaName, String inputTableName) {
        Optional<SourceReference> maybeSourceRef = sourceReferenceService.getSourceReference(inputSchemaName, inputTableName);
        if (!maybeSourceRef.isPresent()) {
            // Explicitly throw to give a better exception message
            throw new RuntimeException(format("No schema found for %s/%s", inputSchemaName, inputTableName));
        }
        return maybeSourceRef.get();
    }

    @VisibleForTesting
    static Dataset<Row> latestRecords(Dataset<Row> df, SourceReference.PrimaryKey primaryKey) {
        val primaryKeys = JavaConverters
                .asScalaIteratorConverter(primaryKey.getKeyColumnNames().stream().map(functions::col).iterator())
                .asScala()
                .toSeq();
        val window = Window.partitionBy(primaryKeys)
                .orderBy(
                        unix_timestamp(
                                col(TIMESTAMP),
                                // for timestamp format see https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.Configuring
                                // todo check if we need to do anything timezone related
                                "yyyy-MM-dd HH:mm:ss.SSSSSS"
                        ).cast(TimestampType).desc()
                );

        return df
                .withColumn("rank", rank().over(window))
                .where("rank = 1")
                .drop("rank");
    }

    @VisibleForTesting
    static boolean schemaIsValid(StructType schema, SourceReference sourceReference) {
        val schemaFields = sourceReference.getSchema().fields();

        val dataFields = Arrays
                .stream(schema.fields())
                .collect(Collectors.toMap(StructField::name, StructField::dataType));

        val requiredFields = Arrays.stream(schemaFields)
                .filter(field -> !field.nullable())
                .collect(Collectors.toList());

        val missingRequiredFields = requiredFields
                .stream()
                .filter(field -> dataFields.get(field.name()) == null)
                .collect(Collectors.toList());

        val invalidRequiredFields = requiredFields
                .stream()
                .filter(field -> dataFields.get(field.name()) != field.dataType())
                .collect(Collectors.toList());

        val nullableFields = Arrays.stream(schemaFields)
                .filter(StructField::nullable)
                .collect(Collectors.toList());

        val invalidNullableFields = nullableFields
                .stream()
                .filter(field -> dataFields.get(field.name()) != field.dataType())
                .collect(Collectors.toList());

        return (missingRequiredFields.isEmpty() || invalidRequiredFields.isEmpty() || invalidNullableFields.isEmpty());
    }
}
