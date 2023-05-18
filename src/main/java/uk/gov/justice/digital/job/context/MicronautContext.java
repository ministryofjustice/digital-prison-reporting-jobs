package uk.gov.justice.digital.job.context;

import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;

public class MicronautContext {

    /**
     * Creates and starts a Micronaut context populated with the supplied args.
     *
     * @param args
     * @return
     */
    public static ApplicationContext withArgs(String[] args) {
        return Micronaut
                .build(args)
                .banner(false)
                .start();
    }

    private MicronautContext() { }

}
