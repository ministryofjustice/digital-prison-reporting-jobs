package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import lombok.val;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.exception.TableDiscoveryException;

import javax.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class TableDiscoveryService {

    private final JobArguments arguments;
    private final ConfigService configService;

    @Inject
    public TableDiscoveryService(JobArguments arguments, ConfigService configService) {
        this.arguments = arguments;
        this.configService = configService;
    }

    private static final Logger logger = LoggerFactory.getLogger(TableDiscoveryService.class);

    public List<ImmutablePair<String, String>> discoverTablesToProcess() {
        return configService.getConfiguredTables(arguments.getConfigKey()).asList();
    }

    public Map<ImmutablePair<String, String>, List<String>> discoverBatchFilesToLoad(String s3Path, SparkSession sparkSession) throws TableDiscoveryException {
        val listPathsStartTime = System.currentTimeMillis();
        String fileGlobPattern = arguments.getBatchLoadFileGlobPattern();
        logger.info("Enumerating load files using glob pattern {}", fileGlobPattern);
        try {
            FileSystem fileSystem = FileSystem.get(URI.create(s3Path), sparkSession.sparkContext().hadoopConfiguration());
            List<ImmutablePair<String, String>> tablesToProcess = discoverTablesToProcess();

            Map<ImmutablePair<String, String>, List<String>> pathsByTable = new HashMap<>();

            for (val tableToProcess : tablesToProcess) {
                String schema = tableToProcess.getLeft();
                String table = tableToProcess.getRight();
                String tablePath = tablePath(s3Path, schema, table);
                List<String> filePathsToProcess = listFiles(fileSystem, tablePath, fileGlobPattern);
                if(!filePathsToProcess.isEmpty()) {
                    val key = new ImmutablePair<>(schema, table);
                    pathsByTable.put(key, filePathsToProcess);
                } else {
                    logger.info("Found no files to process for {}.{} in {}", schema, table, s3Path);
                }
            }
            logger.info("Finished enumerating load files in {}ms", System.currentTimeMillis() - listPathsStartTime);
            return pathsByTable;
        } catch (IOException e) {
            throw new TableDiscoveryException(e);
        }

    }

    public List<String> listFiles(FileSystem fs, String tablePath, String fileGlobPattern) throws TableDiscoveryException {
        logger.info("Listing files with path {} and glob pattern {}", tablePath, fileGlobPattern);
        Path tablePathGlob = new Path(tablePath, fileGlobPattern);
        try {
            FileStatus[] fileStatuses = fs.globStatus(tablePathGlob);
            if (fileStatuses != null) {
               return Arrays.stream(fileStatuses)
                        .filter(FileStatus::isFile)
                        .map(f -> f.getPath().toString())
                        .peek(filePath -> logger.info("Processing file {}", filePath))
                        .collect(Collectors.toList());
            } else {
                return Collections.emptyList();
            }
        } catch (IOException e) {
            throw new TableDiscoveryException(e);
        }
    }
}
