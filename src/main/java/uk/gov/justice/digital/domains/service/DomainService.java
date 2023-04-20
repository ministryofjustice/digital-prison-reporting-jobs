package uk.gov.justice.digital.domains.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.justice.digital.common.Util;
import uk.gov.justice.digital.domains.model.TableInfo;
import uk.gov.justice.digital.domains.model.TableTuple;
import uk.gov.justice.digital.service.DataStorageService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Logger;

public class DomainService extends Util {

    protected DataStorageService storageService = new DataStorageService();
    static Logger logger = Logger.getLogger(DomainService.class.getName());


    public void saveFull(final TableInfo info, final Dataset<Row> dataFrame) {
        logger.info("DomainExecutor:: saveFull");
        storageService.replace(info.getPrefix(), info.getSchema(), info.getTable(), dataFrame);
        storageService.endTableUpdates(info.getPrefix(), info.getSchema(), info.getTable());
        storageService.vacuum(info.getPrefix(), info.getSchema(), info.getTable());
    }

    protected void saveViolations(final TableInfo target, final Dataset<Row> dataFrame) {
        // save the violations to the specified location
        storageService.append(target.getPrefix(), target.getSchema(), target.getTable(), dataFrame);
        storageService.endTableUpdates(target.getPrefix(), target.getSchema(), target.getTable());
    }

    public void deleteFull(final TableInfo info) {
        logger.info("DomainExecutor:: deleteFull");
        storageService.delete(info.getPrefix(), info.getSchema(), info.getTable());
        storageService.vacuum(info.getPrefix(), info.getSchema(), info.getTable());
    }

    public Dataset<Row> getAllSourcesForTable(final String sourcePath, final String source,
                                              final TableTuple exclude) {
        if(exclude != null && exclude.asString().equalsIgnoreCase(source)) {
            //TODO: this condition only for unit test
            // we already have this table
            logger.info("table already present " + exclude.asString());
        } else {
            try {
                TableTuple full = new TableTuple(source);
                final Dataset<Row> dataFrame = storageService.load(sourcePath, full.getSchema(), full.getTable());
                if(dataFrame != null) {
                    logger.info("Loaded source '" + full.asString() +"'.");
                    return dataFrame;
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
        System.err.print(sw.getBuffer().toString());
    }

}
