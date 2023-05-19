package uk.gov.justice.digital.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;

public class DomainSchemaValidationTest {

    private static final String RESOURCE_PATH = "/schemas";
    private static final String AGENCY_INTERNAL_LOCATIONS_SCHEMA = "agency-internal-locations-schema.json";
    private static final String DOMAIN_CONTRACT_SCHEMA = "domain-contract-schema.json";

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final JsonSchema validator = JsonSchemaFactory
            .getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(getResource(RESOURCE_PATH + "/" + DOMAIN_CONTRACT_SCHEMA));

    @Test
    public void agencyInternalLocationsSchemaShouldPassValidation() throws JsonProcessingException {
        val domain = mapper.readTree(getResource(RESOURCE_PATH + "/" + AGENCY_INTERNAL_LOCATIONS_SCHEMA));
        assertEquals(Collections.emptySet(), validator.validate(domain));
    }

}
