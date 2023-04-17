package uk.gov.justice.digital.domains.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.justice.digital.domains.model.TableInfo;
import uk.gov.justice.digital.service.DeltaLakeService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.logging.Logger;

public class DomainService {

    protected DeltaLakeService deltaService = new DeltaLakeService();
    static Logger logger = Logger.getLogger(DomainService.class.getName());
    public void saveFull(final TableInfo info, final Dataset<Row> dataFrame) {
        logger.info("DomainExecutor:: saveFull");
        deltaService.replace(info.getPrefix(), info.getSchema(), info.getTable(), dataFrame);
        deltaService.endTableUpdates(info.getPrefix(), info.getSchema(), info.getTable());
        deltaService.vacuum(info.getPrefix(), info.getSchema(), info.getTable());
    }

    protected void saveViolations(final TableInfo target, final Dataset<Row> dataFrame) {
        // save the violations to the specified location
        deltaService.append(target.getPrefix(), target.getSchema(), target.getTable(), dataFrame);
        deltaService.endTableUpdates(target.getPrefix(), target.getSchema(), target.getTable());
    }

    protected void handleError(final Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        System.err.print(sw.getBuffer().toString());
    }

    protected boolean schemaContains(final Dataset<Row> dataFrame, final String field) {
        return Arrays.asList(dataFrame.schema().fieldNames()).contains(field);
    }
}
