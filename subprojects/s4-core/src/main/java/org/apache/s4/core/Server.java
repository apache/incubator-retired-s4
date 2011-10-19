package org.apache.s4.core;

import java.net.URL;


import org.apache.s4.base.util.JarLoader;
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


        // MAKE SURE YOU COPY THE RESOURCE TO THE CLASSPATH 
        // example: subprojects/s4-core/bin/apps/MY_RESOURCE (in Eclipse)
        //String resource = "/apps/HelloApp.jar";
        String resource = "/apps/CounterExample.s4r";
        // Read the jar as a resource into a URL.
        URL url = this.getClass().getResource(resource);
        if(url == null) {
            logger.error("Couldn't read resource.");
            System.exit(-1);
        }
        logger.trace("Read: {}", url.toString());
        
        // Convert the URL to a File and load the jar.
        JarLoader cl = new JarLoader(url.getFile());
        
//        URL url = this.getClass().getResource("/apps/HelloApp.jar");
//        logger.trace("URL: " + url.toString());
        
        // LOOK AT the custom classloader class MultiClassLoader, for now it

        String appClassName = "io.s4.example.counter.MyApp"; // THE APP CLASS WE NEED TO GET FROM THE MANIFEST.
        App myApp=null;
        
        try {
            Object o = (cl.loadClass(appClassName)).newInstance();
            myApp = (App) o;
        } catch (Exception e) {
            System.out.println("Caught exception : " + e);
            e.printStackTrace();
        }
        myApp.start();
        
        Sender sender = injector.getInstance(Sender.class);
        Receiver receiver = injector.getInstance(Receiver.class);
        myApp.setCommLayer(sender, receiver);
        myApp.init();
        myApp.start();
    }
}
