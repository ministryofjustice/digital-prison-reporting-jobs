package uk.gov.justice.digital.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domains.model.TableInfo;
import uk.gov.justice.digital.domains.model.TableTuple;
import uk.gov.justice.digital.exceptions.DomainExecutorException;

import java.io.PrintWriter;
import java.io.StringWriter;

public class DomainOperations {

    protected DataStorageService deltaService = new DataStorageService();
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainOperations.class);


    public void saveFull(final TableInfo info, final Dataset<Row> dataFrame) {
        logger.info("DomainOperations:: saveFull");
        String tablePath = deltaService.getTablePath(info.getPrefix(), info.getSchema(), info.getTable());
        deltaService.replace(tablePath, dataFrame);
        deltaService.endTableUpdates(info);
        deltaService.vacuum(info);
    }

    protected void saveViolations(final TableInfo target, final Dataset<Row> dataFrame) {
        String tablePath = deltaService.getTablePath(target.getPrefix(), target.getSchema(), target.getTable());
        // save the violations to the specified location
        deltaService.append(tablePath, dataFrame);
        deltaService.endTableUpdates(target);
    }

    public void deleteFull(final TableInfo info) {
        logger.info("DomainOperations:: deleteFull");
        deltaService.delete(info);
        deltaService.vacuum(info);
    }

    public Dataset<Row> getAllSourcesForTable(final String sourcePath, final String source,
                                              final TableTuple exclude) throws DomainExecutorException {
        if(exclude != null && exclude.asString().equalsIgnoreCase(source)) {
            //TODO: this condition only for unit test
            // we already have this table
            logger.info("table already present " + exclude.asString());
        } else {
            try {
                TableTuple full = new TableTuple(source);
                final Dataset<Row> dataFrame = deltaService.load(
                        TableInfo.create(sourcePath, full.getSchema(), full.getTable()));
                if(dataFrame != null) {
                    logger.info("Loaded source '" + full.asString() +"'.");
                    return dataFrame;
                } else {
                    logger.error("Source " + full.asString() + " not found");
                    throw new DomainExecutorException("Source " + full.asString() + " not found");
                }
            } catch(Exception e) {
                handleError(e);
            }
        }
        return null;
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        logger.error(sw.getBuffer().toString());
    }

}
