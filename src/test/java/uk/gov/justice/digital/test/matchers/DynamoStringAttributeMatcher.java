package uk.gov.justice.digital.test.matchers;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class DynamoStringAttributeMatcher extends TypeSafeMatcher<AttributeValue> {

    private final String expectedValue;

    public DynamoStringAttributeMatcher(String expectedValue) {
        this.expectedValue = expectedValue;
    }

    @Override
    protected boolean matchesSafely(AttributeValue attributeValue) {
        return attributeValue.getS().equals(expectedValue);
    }

    @Override
    public void describeTo(Description description) {
        description
                .appendText("AttributeValue has String value ")
                .appendText(expectedValue);
    }

}
