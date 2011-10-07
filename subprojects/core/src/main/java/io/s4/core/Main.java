package io.s4.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class Main {

    /**
     * Starts an S4 server.
     * 
     * @param args
     */
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Main.class);

        /*
         * Need to get name of plugin module. Load ControllerModule to get
         * configuration.
         */
        Injector injector = Guice.createInjector(new io.s4.core.Module());

        Server controller = injector.getInstance(Server.class);
        try {
            controller.start();
        } catch (Exception e) {
            logger.error("Failed to start the controller.", e);
        }

    }
}
