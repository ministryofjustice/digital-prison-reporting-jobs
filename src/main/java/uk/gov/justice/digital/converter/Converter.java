package uk.gov.justice.digital.converter;

public interface Converter<I, O> {

    O convert(I input);

}
