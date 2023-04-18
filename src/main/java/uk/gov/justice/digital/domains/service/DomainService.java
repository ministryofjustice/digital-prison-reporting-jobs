package uk.gov.justice.digital.domains.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import uk.gov.justice.digital.common.Util;
import uk.gov.justice.digital.domains.model.TableInfo;
import uk.gov.justice.digital.service.DeltaLakeService;

import java.util.logging.Logger;

public class DomainService extends Util {

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


}
