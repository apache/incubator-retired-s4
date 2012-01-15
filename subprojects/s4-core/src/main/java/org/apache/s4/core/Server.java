package org.apache.s4.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;

import org.apache.s4.base.util.S4RLoader;
import org.apache.s4.deploy.DeploymentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.common.io.PatternFilenameFilter;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

/**
 * The Server instance coordinates activities in a cluster node including loading and unloading of applications and
 * instantiating the communication layer.
 */
public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final String commModuleName;
    private final String logLevel;
    public static final String MANIFEST_S4_APP_CLASS = "S4-App-Class";
    // local applications directory
    private final String appsDir;
    List<App> apps = new ArrayList<App>();
    CountDownLatch signalOneAppLoaded = new CountDownLatch(1);

    private Injector injector;

    @Inject
    private DeploymentManager deploymentManager;

    /**
     * 
     */
    @Inject
    public Server(@Named("comm.module") String commModuleName, @Named("s4.logger_level") String logLevel,
            @Named("appsDir") String appsDir) {
        this.commModuleName = commModuleName;
        this.logLevel = logLevel;
        this.appsDir = appsDir;
    }

    public void start() throws Exception {

        /* Set up logger basic configuration. */
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(logLevel));

        AbstractModule module = null;

        /* Initialize communication layer module. */
        try {
            module = (AbstractModule) Class.forName(commModuleName).newInstance();
        } catch (Exception e) {
            logger.error("Unable to instantiate communication layer module.", e);
        }

        /* After some indirection we get the injector. */
        injector = Guice.createInjector(module);
        Thread.sleep(10000);

        File[] s4rFiles = new File(appsDir).listFiles(new PatternFilenameFilter("\\w+\\.s4r"));
        for (File s4rFile : s4rFiles) {
            loadApp(s4rFile);
        }

        /* Now init + start apps. TODO: implement dynamic loading/unloading using ZK. */
        for (App app : apps) {
            logger.info("Starting app " + app.getClass().getName());
            startApp(app);
        }

        EventSource savedES = null;
        App consumerApp = null;
        for (App app : apps) {
            logger.info("Resolving dependencies for " + app.getClass().getName());
            List<EventSource> eventSources = app.getEventSources();
            if (eventSources.size() > 0) {
                EventSource es = eventSources.get(0);
                logger.info("App [{}] exports event source [{}].", app.getClass().getName(), es.getName());
                savedES = es; // hardcoded
            } else {

                // hardcoded (one app has event source the other one doesn't.
                consumerApp = app;
            }
        }
        // hardcoded: make savedApp subscribe to savedES
        logger.info("The consumer app is [{}].", consumerApp.getClass().getName());
        // get the list of streams and find the one we are looking for that has name: "I need the time."
        List<Streamable> streams = consumerApp.getStreams();
        for (Streamable aStream : streams) {

            String streamName = aStream.getName();

            if (streamName.contentEquals("I need the time.")) {
                logger.info("Subscribing stream [{}] from app [{}] to event source.", streamName, consumerApp
                        .getClass().getName());
                savedES.subscribeStream(aStream);
            }
        }

        logger.info("Completed local applications startup.");

        if (deploymentManager != null) {
            deploymentManager.start();
        }

        // wait for at least 1 app to be loaded (otherwise the server would not have anything to do and just die)
        signalOneAppLoaded.await();

    }

    public String getS4RDir() {
        return appsDir;
    }

    public App loadApp(File s4r) {

        Sender sender = injector.getInstance(Sender.class);
        Receiver receiver = injector.getInstance(Receiver.class);

        S4RLoader cl = new S4RLoader(s4r.getAbsolutePath());
        try {
            JarFile s4rFile = new JarFile(s4r);
            if (s4rFile.getManifest() == null) {
                logger.warn("Cannot load s4r archive [{}] : missing manifest file");
                return null;
            }
            if (!s4rFile.getManifest().getMainAttributes().containsKey(new Name(MANIFEST_S4_APP_CLASS))) {
                logger.warn("Cannot load s4r archive [{}] : missing attribute [{}] in manifest", s4r.getAbsolutePath(),
                        MANIFEST_S4_APP_CLASS);
                return null;
            }
            String appClassName = s4rFile.getManifest().getMainAttributes().getValue(MANIFEST_S4_APP_CLASS);
            logger.info("App class name is: " + appClassName);
            App app = null;

            try {
                Object o = (cl.loadClass(appClassName)).newInstance();
                app = (App) o;
            } catch (Exception e) {
                logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
                return null;
            }

            app.setCommLayer(sender, receiver);
            apps.add(app);
            logger.info("Loaded application from file {}", s4r.getAbsolutePath());
            signalOneAppLoaded.countDown();
            return app;
        } catch (IOException e) {
            logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
            return null;
        }

    }

    public void startApp(App app) {
        app.init();
        app.start();
    }
}
