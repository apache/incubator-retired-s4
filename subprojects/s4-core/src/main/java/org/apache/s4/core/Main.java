package org.apache.s4.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;
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
        try {
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
        } catch (Exception e) {
            logger.error("Cannot start S4 node", e);
        }
    }

    private static void startCustomS4Node(String s4PropertiesFilePath) throws FileNotFoundException {
        // TODO that's quite inconvenient anyway: we still need to specify the comm module in the config
        // file passed as a parameter...
        Injector injector = Guice
                .createInjector(new DefaultModule(new FileInputStream(new File(s4PropertiesFilePath))));
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
            server.start(injector);
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
        if (args.length < 2 && args.length > 3) {
            usageForDevelopmentMode(args);
        }
        logger.info("Starting S4 app with module [{}] and app [{}]", args[0], args[1]);
        Injector injector = null;
        try {
            if (!AbstractModule.class.isAssignableFrom(Class.forName(args[0]))) {
                logger.error("Module class [{}] is not an instance of [{}]", args[0], AbstractModule.class.getName());
                System.exit(-1);
            }
            if (args.length == 3) {
                if (!(new File(args[2]).exists())) {
                    logger.error("Cannot find S4 config file {}", args[2]);
                    System.exit(-1);
                }
                try {
                    injector = Guice.createInjector((AbstractModule) Class.forName(args[0])
                            .getConstructor(InputStream.class).newInstance(new FileInputStream(new File(args[2]))));
                } catch (Exception e) {
                    logger.error("Module loading error", e);
                    System.exit(-1);
                }
            } else {
                URL defaultS4Config = null;
                try {
                    defaultS4Config = Resources.getResource("default.s4.properties");
                } catch (IllegalArgumentException e) {
                    logger.error(
                            "Module loading error: cannot load default s4 configuration file default.s4.properties from classpath",
                            e);
                    System.exit(-1);
                }

                try {
                    injector = Guice.createInjector((AbstractModule) Class.forName(args[0]).getConstructor(File.class)
                            .newInstance(Resources.newInputStreamSupplier(defaultS4Config).getInput()));
                } catch (Exception e) {
                    logger.error(
                            "Module loading error: cannot load default s4 configuration file default.s4.properties from classpath",
                            e);
                    System.exit(-1);
                }
            }
        } catch (ClassNotFoundException e) {
            logger.error("Invalid module class [{}]", args[0], e);
            System.exit(-1);
        }
        App app;
        try {
            app = (App) injector.getInstance(Class.forName(args[1]));
            app.init();
            app.start();
        } catch (ClassNotFoundException e) {
            logger.error("Invalid S4 application class [{}] : {}", args[1], e.getMessage());
        }
    }

    static void usageForDevelopmentMode(String[] args) {
        logger.info("Invalid parameters "
                + Arrays.toString(args)
                + " \nUsage: java <classpath+params> org.apache.s4.core.Main <moduleClassName> <appClassName>"
                + "\n(this uses default.s4.properties from the classpath)"
                + "\nor:"
                + " java <classpath+params> org.apache.s4.core.Main <moduleClassName> <appClassName> <pathToConfigFile>");
        System.exit(-1);
    }
}
