package uk.gov.justice.digital.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.justice.digital.client.dynamodb.DomainDefinitionClient;
import uk.gov.justice.digital.config.JobArguments;
import uk.gov.justice.digital.domain.DomainExecutor;
import uk.gov.justice.digital.domain.model.DomainDefinition;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DomainServiceTest {

    private static final String domainName = "SomeDomain";
    private static final String domainTableName = "SomeDomainTable";

    @Mock private JobArguments mockJobArguments;
    @Mock private DomainDefinitionClient mockDomainDefinitionClient;
    @Mock private DomainExecutor mockDomainExecutor;
    @Mock private DomainDefinition mockDomainDefinition;

    private DomainService underTest;

    @BeforeEach
    public void setup() {
        underTest = new DomainService(mockJobArguments, mockDomainDefinitionClient, mockDomainExecutor);
    }

    @Test
    public void shouldRunDeleteWhenOperationIsDelete() throws Exception {
        givenJobArgumentsWithOperation("delete");

        underTest.run();

        verify(mockDomainExecutor).doDomainDelete(domainName, domainTableName);
    }

    @Test
    public void shouldRunFullRefreshWhenOperationIsInsert() throws Exception {
        givenJobArgumentsWithOperation("insert");
        givenTheClientReturnsADomainDefinition();

        underTest.run();

        verify(mockDomainExecutor).doFullDomainRefresh(mockDomainDefinition, domainTableName, "insert");
    }

    private void givenJobArgumentsWithOperation(String operation) {
        when(mockJobArguments.getDomainTableName()).thenReturn(domainTableName);
        when(mockJobArguments.getDomainName()).thenReturn(domainName);
        when(mockJobArguments.getDomainOperation()).thenReturn(operation);
    }

    private void givenTheClientReturnsADomainDefinition() throws Exception {
        when(mockDomainDefinition.getName()).thenReturn(domainName);
        when(mockDomainDefinitionClient.getDomainDefinition(domainName, domainTableName)).thenReturn(mockDomainDefinition);
    }

}
