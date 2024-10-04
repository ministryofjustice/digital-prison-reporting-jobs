package uk.gov.justice.digital.service;

import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.DataStorageException;
import uk.gov.justice.digital.exception.MaintenanceOperationFailedException;

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * Responsible for performing maintenance tasks.
 */
public class MaintenanceService {

    private static final Logger logger = LoggerFactory.getLogger(MaintenanceService.class);
    private final DataStorageService storageService;

    @Inject
    public MaintenanceService(DataStorageService storageService) {
        this.storageService = storageService;
    }

    /**
     * Runs a delta lake compaction on all delta lake tables recursively below rootPath with the given depth limit
     */
    public void compactDeltaTables(SparkSession spark, String rootPath, ImmutableSet<String> configuredTablePaths, int recurseForTablesDepthLimit) throws DataStorageException, MaintenanceOperationFailedException {
        logger.info("Beginning delta table compaction for tables under root path: {}", rootPath);
        List<String> deltaTablePaths = storageService.listDeltaTablePaths(spark, rootPath, recurseForTablesDepthLimit);
        List<String> filteredDeltaTablePaths = configuredTablePaths.isEmpty()
                ? deltaTablePaths
                : filterPathsForConfig(configuredTablePaths, deltaTablePaths);
        logger.info("Found {} delta tables", filteredDeltaTablePaths.size());
        String paths = String.join(", ", (filteredDeltaTablePaths));
        logger.debug("Found delta tables at the following paths: {}", paths);
        attemptAll(filteredDeltaTablePaths, path -> storageService.compactDeltaTable(spark, path));
        logger.info("Finished delta table compaction for root path: {}", rootPath);
    }

    /**
     * Runs a delta lake vacuum on all delta lake tables recursively below rootPath with the given depth limit
     */
    public void vacuumDeltaTables(SparkSession spark, String rootPath, ImmutableSet<String> configuredTablePaths, int recurseForTablesDepthLimit) throws DataStorageException, MaintenanceOperationFailedException {
        logger.info("Beginning delta table vacuum for tables under root path {}", rootPath);
        List<String> deltaTablePaths = storageService.listDeltaTablePaths(spark, rootPath, recurseForTablesDepthLimit);
        List<String> filteredDeltaTablePaths = configuredTablePaths.isEmpty()
                ? deltaTablePaths
                : filterPathsForConfig(configuredTablePaths, deltaTablePaths);

        logger.info("Found {} delta tables", filteredDeltaTablePaths.size());
        String paths = String.join(", ", (filteredDeltaTablePaths));
        logger.debug("Found delta tables at the following paths: {}", paths);
        attemptAll(filteredDeltaTablePaths, path -> storageService.vacuum(spark, path));
        logger.info("Finished delta table vacuum for tables under root path: {}", rootPath);
    }

    /**
     * Specialised functional interface to allow throwing checked Exceptions which is not available in the Java stdlib
     */
    @FunctionalInterface
    private interface MaintenanceOperation {
        void apply(String path);
    }

    /**
     * Attempts the maintenance operation on every path, skipping failed operations and only throwing at the end if any
     * maintenance operation failed.
     * @throws MaintenanceOperationFailedException If any maintenance operation failed
     */
    private static void attemptAll(Iterable<String> paths, MaintenanceOperation f) throws MaintenanceOperationFailedException {
        int numFailed = 0;
        for (String path: paths) {
            try {
                f.apply(path);
            } catch (Exception e) {
                numFailed++;
                logger.error(format("Failed maintenance operation on %s", path), e);
            }
        }
        if(numFailed != 0) {
            String msg = format("Finished maintenance operation with %d failures", numFailed);
            logger.error(msg);
            throw new MaintenanceOperationFailedException(msg);
        }
    }

    @NotNull
    private static List<String> filterPathsForConfig(ImmutableSet<String> configuredTablePaths, List<String> deltaTablePaths) {
        return deltaTablePaths.stream()
                .filter(path -> configuredTablePaths.stream().anyMatch(configuredTablePath -> path.toLowerCase().contains(configuredTablePath.toLowerCase())))
                .collect(Collectors.toList());
    }
}
