package uk.gov.justice.digital.job;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import uk.gov.justice.digital.client.s3.S3DataProvider;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.config.JobProperties;
import uk.gov.justice.digital.datahub.model.SourceReference;
import uk.gov.justice.digital.exception.BackfillException;
import uk.gov.justice.digital.exception.SchemaNotFoundException;
import uk.gov.justice.digital.job.batchprocessing.ArchiveBackfillProcessor;
import uk.gov.justice.digital.provider.SparkSessionProvider;
import uk.gov.justice.digital.service.ConfigService;
import uk.gov.justice.digital.service.SourceReferenceService;
import uk.gov.justice.digital.service.TableDiscoveryService;

import javax.inject.Inject;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static uk.gov.justice.digital.common.ResourcePath.createValidatedPath;

/**
 * Job that creates a copy of the archive data retroactively processed to ensure data consistency without compromising integrity.
 * For example, when nullable columns are added to the schema contract, a back-fill is created with the new columns but with a default value of null.
 */
@CommandLine.Command(name = "ArchiveBackfillJob")
public class ArchiveBackfillJob implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ArchiveBackfillJob.class);
    private final JobArguments jobArguments;
    private final JobProperties properties;
    private final ConfigService configService;
    private final S3DataProvider dataProvider;
    private final SparkSessionProvider sparkSessionProvider;
    private final TableDiscoveryService tableDiscoveryService;
    private final ArchiveBackfillProcessor archiveBackfillProcessor;
    private final SourceReferenceService sourceReferenceService;

    @Inject
    public ArchiveBackfillJob(
            JobArguments jobArguments,
            JobProperties properties,
            ConfigService configService,
            S3DataProvider dataProvider,
            SparkSessionProvider sparkSessionProvider,
            TableDiscoveryService tableDiscoveryService,
            ArchiveBackfillProcessor archiveBackfillProcessor,
            SourceReferenceService sourceReferenceService
    ) {
        this.jobArguments = jobArguments;
        this.dataProvider = dataProvider;
        this.properties = properties;
        this.configService = configService;
        this.sparkSessionProvider = sparkSessionProvider;
        this.tableDiscoveryService = tableDiscoveryService;
        this.archiveBackfillProcessor = archiveBackfillProcessor;
        this.sourceReferenceService = sourceReferenceService;
    }

    public static void main(String[] args) {
        PicocliMicronautExecutor.execute(ArchiveBackfillJob.class, args);
    }

    @Override
    public void run() {
        SparkJobRunner.run("ArchiveBackfillJob", jobArguments, properties, sparkSessionProvider, logger, this::runJob);
    }

    @VisibleForTesting
    void runJob(SparkSession sparkSession) throws RuntimeException {
        String configKey = jobArguments.getConfigKey();
        ImmutableSet<ImmutablePair<String, String>> configuredTables = configService.getConfiguredTables(configKey);
        List<SourceReference> sourceReferences = sourceReferenceService.getAllSourceReferences(configuredTables);

        if (sourceReferences.isEmpty()) {
            String errorMessage = String.format("No schemas found for domain %s", configKey);
            logger.warn(errorMessage);
            throw new SchemaNotFoundException(errorMessage);
        } else {
            val archiveFilesPathsByTable = tableDiscoveryService
                    .discoverBatchFilesToLoad(jobArguments.getRawArchiveS3Path(), sparkSession);

            for (val sourceReference : sourceReferences) {
                val source = sourceReference.getSource();
                val table = sourceReference.getTable();
                val tableKey = ImmutablePair.of(source, table);

                if (archiveFilesPathsByTable.containsKey(tableKey)) {
                    logger.info("Creating back-filled archive for table {}.{}", source, table);
                    val archiveFilePaths = Optional.ofNullable(archiveFilesPathsByTable.get(tableKey))
                            .orElse(Collections.emptyList());

                    val outputPath = createValidatedPath(jobArguments.getTempReloadS3Path(), jobArguments.getTempReloadOutputFolder());
                    if (archiveFilePaths.isEmpty()) {
                        throw new BackfillException("No archive files discovered for table " + source + "." + table);
                    } else {
                        val archiveDataset = dataProvider.getBatchSourceData(sparkSession, archiveFilePaths);
                        archiveBackfillProcessor.createBackfilledArchiveData(sourceReference, outputPath, archiveDataset);
                    }
                } else {
                    logger.info("Excluding table {}.{} which is not in archive", source, table);
                }
            }
        }
    }
}
