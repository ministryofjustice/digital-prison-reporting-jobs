package uk.gov.justice.digital.service;

import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.gov.justice.digital.config.JobParameters;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.repository.DomainRepository;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

@Singleton
public class DomainService {

    private final DomainRepository repo;
    private final DomainExecutor executor;

    protected JobParameters parameters;

    private static final Logger logger = LoggerFactory.getLogger(DomainService.class);

    @Inject
    public DomainService(JobParameters parameters,
                        DomainRepository repository,
                         DomainExecutor executor) {
        this.parameters = parameters;
        this.repo = repository;
        this.executor = executor;
    }

    public void run() {
        runInternal(
                parameters.getDomainRegistry(),
                parameters.getDomainTableName(),
                parameters.getDomainName(),
                parameters.getDomainOperation()
        );
    }

    protected void runInternal(
        String domainRegistry,
        String domainTableName,
        String domainName,
        String domainOperation
    ) throws PatternSyntaxException {
        if (domainOperation.equalsIgnoreCase("delete")) {
            // TODO - instead of passing null private an alternate method/overload
            processDomain(null, domainName, domainTableName, domainOperation);
        }
        else {
            val domains = getDomains(domainRegistry, domainName);

            logger.info("Located " + domains.size() + " domains for name '" + domainName + "'");
            for(val domain : domains) {
                processDomain(domain, domain.getName(), domainTableName, domainOperation);
            }
        }

    }

    private Set<DomainDefinition> getDomains(String domainRegistry, String domainName) throws PatternSyntaxException {
        return repo.getForName(domainRegistry, domainName);
    }

    protected void processDomain(
        DomainDefinition domain,
        String domainName,
        String domainTableName,
        String domainOperation
    ) {
        val prefix = "processing of domain: '" + domainName + "' operation: " + domainOperation + " ";

        try {
            logger.info(prefix + "started");
            executor.doFullDomainRefresh(domain, domainName, domainTableName, domainOperation);
            logger.info(prefix + "completed");
        } catch(Exception e) {
            logger.error(prefix + "failed", e);
        }
    }
}
