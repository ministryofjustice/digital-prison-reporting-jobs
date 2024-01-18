package uk.gov.justice.digital.test.matchers;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.hamcrest.Matcher;

public class DynamoAttributeMatchers {

    public static Matcher<AttributeValue> hasStringValue(String expectedValue) {
        return new DynamoStringAttributeMatcher(expectedValue);
    }

    public static Matcher<AttributeValue> hasNumberValue(String expectedValue) {
        return new DynamoNumberAttributeMatcher(expectedValue);
    }

    public static Matcher<AttributeValue> hasNumberValue(int expectedValue) {
        return new DynamoNumberAttributeMatcher(Integer.toString(expectedValue));
    }

    public static Matcher<AttributeValue> hasNumberValue(long expectedValue) {
        return new DynamoNumberAttributeMatcher(Long.toString(expectedValue));
    }

    private DynamoAttributeMatchers() {}
}
