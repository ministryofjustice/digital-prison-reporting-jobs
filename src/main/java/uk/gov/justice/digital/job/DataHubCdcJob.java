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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static java.lang.String.format;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.unix_timestamp;
import static org.apache.spark.sql.types.DataTypes.TimestampType;
import static uk.gov.justice.digital.common.ResourcePath.ensureEndsWithSlash;
import static uk.gov.justice.digital.config.JobProperties.SPARK_JOB_NAME_PROPERTY;
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
        Dataset<Row> sourceDf = s3DataProvider.getSourceData(spark, arguments, inputSchemaName, inputTableName);
        if(!violationService.dataFrameSchemaIsValid(sourceDf.schema(), sourceReference)) {
            logger.error("Schema for {}/{} is not valid\n{}", inputSchemaName, inputTableName, sourceDf.schema().treeString());
            throw new RuntimeException(format("Schema for %s.%s is not valid %s", inputSchemaName, inputTableName, sourceDf.schema().catalogString()));
        }
        logger.info("Initialising per batch processing for {}/{}", inputSchemaName, inputTableName);

        val structuredTablePath = cdcTablePath(arguments.getStructuredS3Path(), sourceReference);
        val curatedTablePath = cdcTablePath(arguments.getCuratedS3Path(), sourceReference);
        String queryName = format("Datahub CDC %s.%s", inputSchemaName, inputTableName);
        String queryCheckpointPath = format("%sDataHubCdcJob/%s", ensureEndsWithSlash(arguments.getCheckpointLocation()), queryName);
        logger.info("Initialising query {} with checkpoint path {}", queryName, queryCheckpointPath);

        try {
            sourceDf
                    .writeStream()
                    .queryName(queryName)
                    .format("delta")
                    .foreachBatch((df, batchId) -> {
                        processBatch(sourceReference, spark, df, batchId, structuredTablePath, curatedTablePath);
                    })
                    .outputMode("update")
                    .option("checkpointLocation", queryCheckpointPath)
                    .start();
            logger.info("Started query {}", queryName);
        } catch (TimeoutException e) {
            logger.error("Encountered TimeoutException when running streaming query start", e);
            throw new RuntimeException(e);
        }
    }

    private void processBatch(SourceReference sourceReference, SparkSession spark, Dataset<Row> df, Long batchId, String structuredTablePath, String curatedTablePath) {
        val batchStartTime = System.currentTimeMillis();
        logger.info("Processing batch {} for {}.{}", batchId, sourceReference.getSource(), sourceReference.getTable());
        val primaryKey = sourceReference.getPrimaryKey();
        val latestCDCRecordsByPK = latestRecords(df, primaryKey);
        try {
            storage.writeCdc(spark, structuredTablePath, latestCDCRecordsByPK, primaryKey);
            storage.writeCdc(spark, curatedTablePath, latestCDCRecordsByPK, primaryKey);
        } catch (DataStorageRetriesExhaustedException e) {
            violationService.handleRetriesExhausted(spark, df, sourceReference.getSource(), sourceReference.getTable(), e, CDC);
        }
        logger.info("Processing batch {} {}.{} took {}ms", batchId, sourceReference.getSource(), sourceReference.getTable(), System.currentTimeMillis() - batchStartTime);
    }

    private String cdcTablePath(String zoneRootPath, SourceReference sourceReference) {
        return format("%s%s/%s", ensureEndsWithSlash(zoneRootPath), sourceReference.getSource(), sourceReference.getTable());
    }

    @NotNull
    private static List<ImmutablePair<String, String>> discoverTablesToProcess() {
        // TODO Discover the tables from S3 or fix the broken SourceReferenceService
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
        val window = Window
                .partitionBy(primaryKeys)
                // for timestamp format see https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.Configuring
                // todo check if we need to do anything timezone related
                .orderBy(unix_timestamp(col(TIMESTAMP), "yyyy-MM-dd HH:mm:ss.SSSSSS").cast(TimestampType).desc());

        return df
                .withColumn("row_number", row_number().over(window))
                .where("row_number = 1")
                .drop("row_number");
    }
}
