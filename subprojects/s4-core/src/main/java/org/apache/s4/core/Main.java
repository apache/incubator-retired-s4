package org.apache.s4.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Bootstrap class for S4. It creates an S4 node.
 * 
 */
public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    /**
     * Starts an S4 server.
     * 
     * @param args
     */
    public static void main(String[] args) {

        if (args.length == 0) {
            logger.info("Starting S4 node with default configuration");
            startDefaultS4Node();
        } else if (args.length == 1) {
            logger.info("Starting S4 node with custom configuration from file {}", args[0]);
            startCustomS4Node(args[0]);
        } else {
            logger.info("Starting S4 node in development mode");
            startDevelopmentMode(args);
        }
    }

    private static void startCustomS4Node(String s4PropertiesFilePath) {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        // TODO that's quite inconvenient anyway: we still need to specify the comm module in the config
        // file passed as a parameter...
        Injector injector = Guice.createInjector(new CustomModule(new File(s4PropertiesFilePath)));

        startServer(logger, injector);
    }

    private static void startDefaultS4Node() {
        final Logger logger = LoggerFactory.getLogger(Main.class);

        /*
         * Need to get name of plugin module. Load ControllerModule to get configuration.
         */
        Injector injector = Guice.createInjector(new org.apache.s4.core.Module());

        startServer(logger, injector);
    }

    private static void startServer(final Logger logger, Injector injector) {
        Server server = injector.getInstance(Server.class);
        try {
            server.start();
        } catch (Exception e) {
            logger.error("Failed to start the controller.", e);
        }
    }

    /**
     * Facility for starting S4 apps by passing a module class and an application class
     * 
     * Usage: java &ltclasspath+params&gt org.apache.s4.core.Main &ltappClassName&gt &ltmoduleClassName&gt
     * 
     */
    private static void startDevelopmentMode(String[] args) {
        if (args.length != 2) {
            usageForDevelopmentMode(args);
        }
        logger.info("Starting S4 app with module [{}] and app [{}]", args[0], args[1]);
        Injector injector = null;
        try {
            if (!AbstractModule.class.isAssignableFrom(Class.forName(args[0]))) {
                logger.error("Module class [{}] is not an instance of [{}]", args[0], AbstractModule.class.getName());
                System.exit(-1);
            }
            injector = Guice.createInjector((AbstractModule) Class.forName(args[0]).newInstance());
        } catch (InstantiationException e) {
            logger.error("Invalid app class [{}] : {}", args[0], e.getMessage());
            System.exit(-1);
        } catch (IllegalAccessException e) {
            logger.error("Invalid app class [{}] : {}", args[0], e.getMessage());
            System.exit(-1);
        } catch (ClassNotFoundException e) {
            logger.error("Invalid app class [{}] : {}", args[0], e.getMessage());
            System.exit(-1);
        }
        App app;
        try {
            app = (App) injector.getInstance(Class.forName(args[1]));
            app.init();
            app.start();
        } catch (ClassNotFoundException e) {
            logger.error("Invalid S4 application class [{}] : {}", args[0], e.getMessage());
        }
    }

    static void usageForDevelopmentMode(String[] args) {
        logger.info("Invalid parameters " + Arrays.toString(args)
                + " \nUsage: java <classpath+params> org.apache.s4.core.Main <appClassName> <moduleClassName>");
        System.exit(-1);
    }
}
