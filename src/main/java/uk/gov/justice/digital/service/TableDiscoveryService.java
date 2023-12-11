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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static uk.gov.justice.digital.common.ResourcePath.tablePath;

@Singleton
public class TableDiscoveryService {

    private final JobArguments arguments;

    @Inject
    public TableDiscoveryService(JobArguments arguments) {
        this.arguments = arguments;
    }

    private static final Logger logger = LoggerFactory.getLogger(TableDiscoveryService.class);

    public List<ImmutablePair<String, String>> discoverTablesToProcess() {
        // Tables are initially hardcoded but will be configurable/discovered as part of DPR2-217
        List<ImmutablePair<String, String>> tablesToProcess = new ArrayList<>();
        tablesToProcess.add(new ImmutablePair<>("nomis", "agency_internal_locations"));
        tablesToProcess.add(new ImmutablePair<>("nomis", "agency_locations"));
        tablesToProcess.add(new ImmutablePair<>("nomis", "movement_reasons"));
        tablesToProcess.add(new ImmutablePair<>("nomis", "offender_bookings"));
        tablesToProcess.add(new ImmutablePair<>("nomis", "offender_external_movements"));
        tablesToProcess.add(new ImmutablePair<>("nomis", "offenders"));
        return tablesToProcess;
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
            Optional<List<String>> filePathsToProcess = listFiles(fileSystem, tablePath, fileGlobPattern);
            if(filePathsToProcess.isPresent()) {
                val key = new ImmutablePair<>(schema, table);
                pathsByTable.put(key, filePathsToProcess.get());
            }
        }
        logger.info("Finished enumerating load files in {}ms", System.currentTimeMillis() - listPathsStartTime);
        return pathsByTable;
    }

    public Optional<List<String>> listFiles(FileSystem fs, String tablePath, String fileGlobPattern) throws IOException {
        // todo use empty list instead of option
        Path tablePathGlob = new Path(tablePath, fileGlobPattern);
        logger.info("Listing files with glob pattern {}", tablePathGlob);
        FileStatus[] fileStatuses = fs.globStatus(tablePathGlob);
        if (fileStatuses != null) {
           return Optional.of(Arrays.stream(fileStatuses)
                    .filter(FileStatus::isFile)
                    .map(f -> f.getPath().toString())
                    .peek(filePath -> logger.info("Processing file {}", filePath))
                    .collect(Collectors.toList()));
        } else {
            return Optional.empty();
        }
    }
}
