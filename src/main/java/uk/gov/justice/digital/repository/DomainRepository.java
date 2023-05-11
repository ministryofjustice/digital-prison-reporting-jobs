package uk.gov.justice.digital.repository;

import uk.gov.justice.digital.client.dynamodb.DomainDefinitionDB;
import uk.gov.justice.digital.domain.model.DomainDefinition;
import uk.gov.justice.digital.exception.DomainServiceException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.PatternSyntaxException;

@Singleton
public class DomainRepository {

    private final DomainDefinitionDB dynamoDB;

    @Inject
    public DomainRepository(DomainDefinitionDB dynamoDB) {
        this.dynamoDB = dynamoDB;
    }

    public Set<DomainDefinition> getForName(final String domainRegistry, final String domainTableName)
            throws PatternSyntaxException, DomainServiceException {
        //TODO: The purpose of the Set<> is to have multiple domains. Need change to this code later
        Set<DomainDefinition> domains = new HashSet<>();
        String[] names = domainTableName.split("[.]");
        if (names.length != 2) {
            throw new DomainServiceException("Invalid domain table name. Should be <domain_name>.<table_name>");
        } else {
            DomainDefinition domain = dynamoDB.getDomainDefinition(domainRegistry, names[0], names[1]);
            if (domain != null) {
                domains.add(domain);
            } else {
                throw new DomainServiceException("Database failure");
            }
        }
        return domains;
    }
}
