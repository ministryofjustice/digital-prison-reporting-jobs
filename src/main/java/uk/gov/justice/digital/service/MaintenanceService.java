package uk.gov.justice.digital.service;

import jakarta.inject.Inject;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.exception.DataStorageException;

import java.util.List;

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
     * Runs a delta lake compaction on all delta lake tables immediately below rootPath
     */
    public void compactDeltaTables(SparkSession spark, String rootPath) throws DataStorageException {
        logger.info("Beginning delta table compaction for tables under root path: {}", rootPath);
        List<String> deltaTablePaths = storageService.listDeltaTablePaths(spark, rootPath);
        logger.info("Found delta tables at: {}", String.join(", ", (deltaTablePaths)));
        for (String deltaTablePath : deltaTablePaths) {
            storageService.compactDeltaTable(spark, deltaTablePath);
        }
        logger.info("Finished delta table compaction for root path: {}", rootPath);
    }

    /**
     * Runs a delta lake vacuum on all delta lake tables immediately below rootPath
     */
    public void vacuumDeltaTables(SparkSession spark, String rootPath) throws DataStorageException {
        logger.info("Beginning delta table vacuum for tables under root path {}", rootPath);
        List<String> deltaTablePaths = storageService.listDeltaTablePaths(spark, rootPath);
        logger.info("Found delta tables at: {}", String.join(", ", (deltaTablePaths)));
        for (String deltaTablePath : deltaTablePaths) {
            storageService.vacuum(spark, deltaTablePath);
        }
        logger.info("Finished delta table vacuum for tables under root path: {}", rootPath);
    }
}
