package uk.gov.justice.digital.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.model.TableInfo;
import uk.gov.justice.digital.domain.model.TableTuple;
import uk.gov.justice.digital.exception.DomainExecutorException;

public abstract class DomainOperations {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DomainOperations.class);


    public abstract void saveFull(final TableInfo info, final Dataset<Row> dataFrame, final String domainOperation)
            throws DomainExecutorException;

    protected abstract void saveViolations(final TableInfo target, final Dataset<Row> dataFrame);

    public abstract void deleteFull(final TableInfo info) throws DomainExecutorException;

    public abstract Dataset<Row> getAllSourcesForTable(final String sourcePath, final String source,
                                              final TableTuple exclude) throws DomainExecutorException;


}
