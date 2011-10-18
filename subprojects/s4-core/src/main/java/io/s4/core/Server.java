package io.s4.core;

import io.s4.base.util.JarLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

/**
 * The Server instance coordinates activities in a cluster node including
 * loading and unloading of applications and instantiating the communication
 * layer.
 */
public class Server {
    
    final private String moduleName;
    final private String logLevel;

    /**
     * 
     */
    @Inject
    public Server(@Named("comm.module") String moduleName,
            @Named("s4.logger_level") String logLevel) {
        this.moduleName = moduleName;
        this.logLevel = logLevel;
    }

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    public void start() throws Exception {

        /* Set up logger basic configuration. */
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(logLevel));

        Injector injector;
        AbstractModule module = null;

        /* Initialize communication layer module. */
        try {
            module = (AbstractModule) Class.forName(moduleName).newInstance();
        } catch (Exception e) {
            logger.error("Unable to instantiate communication layer module.", e);
        }

        /* After some indirection we get the injector. */
        injector = Guice.createInjector(module);

        // HERE WE SHOULD LOOP TO CHECK IF WE NEED TO LOAD OR UNLOAD APPS.

        logger.trace("Load HelloApp");

        // copy the jar from main/resources/apps/HelloApp.jar to /tmp
        JarLoader cl = new JarLoader("/tmp/HelloApp.jar");
        
        // LOOK AT the custom classloader class MultiClassLoader, for now it

        String tst = "HelloApp";

        try {
            Object o = (cl.loadClass(tst)).newInstance();
            ((App) o).start();
        } catch (Exception e) {
            System.out.println("Caught exception : " + e);
        }
    }
}
