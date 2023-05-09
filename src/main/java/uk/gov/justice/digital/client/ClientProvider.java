package uk.gov.justice.digital.client;

public interface ClientProvider<T> {
    T getClient();
}
