package uk.gov.justice.digital.service.shutdownhooks;

import jakarta.inject.Singleton;

@Singleton
public class ShutdownHookRegistrar {

    public void registerShutdownHook(Runnable shutdownHook) {
        Runtime.getRuntime().addShutdownHook(new Thread(shutdownHook));
    }
}
