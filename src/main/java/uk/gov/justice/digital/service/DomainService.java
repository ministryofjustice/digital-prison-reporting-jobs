package uk.gov.justice.digital.service;

import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.job.Job;
import uk.gov.justice.digital.repository.DomainRepository;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

@Singleton
public class DomainService extends Job {

    private final DomainRepository repo;
    private final DomainExecutor executor;

    protected DataStorageService storage;

    private static final Logger logger = LoggerFactory.getLogger(DomainService.class);

    @Inject
    public DomainService(DomainRepository repository,
                         DataStorageService storage,
                         DomainExecutor executor) {
        this.repo = repository;
        this.storage = storage;
        this.executor = executor;
    }

    public void run(
        String domainRegistry,
        String domainTableName,
        String domainName,
        String domainOperation
    ) throws PatternSyntaxException {
        if (domainOperation.equalsIgnoreCase("delete")) {
            processDomain(domainName, domainTableName, domainOperation);
        }
        else {
            val domains = getDomains(domainRegistry, domainName);
            logger.info("Located " + domains.size() + " domains for name '" + domainName + "'");
            for(DomainDefinition domain : domains) {
                processDomain(domain.getName(), domainTableName, domainOperation);
            }
        }

    }

    protected Set<DomainDefinition> getDomains(String domainRegistry, String domainName)
            throws PatternSyntaxException {
        return repo.getForName(domainRegistry, domainName);
    }

    protected void processDomain(
        String domainName,
        String domainTableName,
        String domainOperation
    ) {
        try {
            logger.info("processing of domain '" + domainName + "' started");
            executor.doFull(domainName, domainTableName, domainOperation);
            logger.info("processing of domain '" + domainName + "' completed");
        } catch(Exception e) {
            logger.error("processing of domain '" + domainName + "' failed", e);
        }
    }

}
