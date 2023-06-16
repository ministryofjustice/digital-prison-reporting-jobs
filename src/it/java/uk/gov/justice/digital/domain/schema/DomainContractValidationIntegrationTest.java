package uk.gov.justice.digital.domain.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.justice.digital.test.ResourceLoader.getResource;

public class DomainContractValidationIntegrationTest {

    private static final String RESOURCE_PATH = "/contracts";

    private static final String DOMAIN_CONTRACT_SCHEMA = "source-contract-schema.json";

    private static final List<String> contracts = Arrays.asList(
            "reference-contract.avsc"
    );

    private static final ObjectMapper jsonParser = new ObjectMapper();

    private static final JsonSchema validator = JsonSchemaFactory
            .getInstance(SpecVersion.VersionFlag.V202012)
            .getSchema(getResource(RESOURCE_PATH + "/" + DOMAIN_CONTRACT_SCHEMA));

    @Test
    public void allContractsShouldValidateAgainstOurContractSchema() {
        contracts.forEach(contract ->
            assertEquals(
                    Collections.emptySet(),
                    validator.validate(parseJson(contract)),
                    "Contract: " + contract + " should validate without errors"
            ));
    }

    private JsonNode parseJson(String contract) {
        try {
            return jsonParser.readTree(getResource(RESOURCE_PATH + "/" + contract));
        }
        catch (Exception e) {
            throw new RuntimeException("Caught exception when reading: " + contract, e);
        }
    }

}
