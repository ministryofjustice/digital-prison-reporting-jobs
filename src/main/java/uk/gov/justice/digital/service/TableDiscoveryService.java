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

import javax.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.util.*;
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

    public Map<ImmutablePair<String, String>, List<String>> discoverBatchFilesToLoad(String rawS3Path, SparkSession sparkSession) throws IOException {
        val listPathsStartTime = System.currentTimeMillis();
        String fileGlobPattern = arguments.getBatchLoadFileGlobPattern();
        logger.info("Enumerating load files using glob pattern {}", fileGlobPattern);
        val fileSystem = FileSystem.get(URI.create(rawS3Path), sparkSession.sparkContext().hadoopConfiguration());
        List<ImmutablePair<String, String>> tablesToProcess = discoverTablesToProcess();

        Map<ImmutablePair<String, String>, List<String>> pathsByTable = new HashMap<>();

        for (val tableToProcess : tablesToProcess) {
            String schema = tableToProcess.getLeft();
            String table = tableToProcess.getRight();
            String tablePath = tablePath(rawS3Path, schema, table);
            List<String> filePathsToProcess = listFiles(fileSystem, tablePath, fileGlobPattern);
            if(!filePathsToProcess.isEmpty()) {
                val key = new ImmutablePair<>(schema, table);
                pathsByTable.put(key, filePathsToProcess);
            }
        }
        logger.info("Finished enumerating load files in {}ms", System.currentTimeMillis() - listPathsStartTime);
        return pathsByTable;
    }

    public List<String> listFiles(FileSystem fs, String tablePath, String fileGlobPattern) throws IOException {
        logger.info("Listing files with path {} and glob pattern {}", tablePath, fileGlobPattern);
        Path tablePathGlob = new Path(tablePath, fileGlobPattern);
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
    }
}
