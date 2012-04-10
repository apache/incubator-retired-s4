package org.apache.s4.core;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.base.util.S4RLoader;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.deploy.DeploymentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.common.collect.Maps;
import com.google.common.io.PatternFilenameFilter;
import com.google.inject.AbstractModule;
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
    Map<String, App> apps = Maps.newHashMap();
    Map<String, Streamable> streams = Maps.newHashMap();
    Map<String, EventSource> eventSources = Maps.newHashMap();
    CountDownLatch signalOneAppLoaded = new CountDownLatch(1);

    private Injector injector;

    @Inject
    private DeploymentManager deploymentManager;

    private String clusterName;

    private ZkClient zkClient;

    /**
     *
     */
    @Inject
    public Server(@Named("comm.module") String commModuleName, @Named("s4.logger_level") String logLevel,
            @Named("appsDir") String appsDir, @Named("cluster.name") String clusterName,
            @Named("cluster.zk_address") String zookeeperAddress,
            @Named("cluster.zk_session_timeout") int sessionTimeout,
            @Named("cluster.zk_connection_timeout") int connectionTimeout) {
        this.commModuleName = commModuleName;
        this.logLevel = logLevel;
        this.appsDir = appsDir;
        this.clusterName = clusterName;

        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        zkClient.setZkSerializer(new ZNRecordSerializer());
    }

    public void start(Injector injector) throws Exception {

        this.injector = injector;
        /* Set up logger basic configuration. */
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(logLevel));

        AbstractModule module = null;

        /* Initialize communication layer module. */
        // TODO do we need a separate comm layer?
        // try {
        // module = (AbstractModule) Class.forName(commModuleName).newInstance();
        // } catch (Exception e) {
        // logger.error("Unable to instantiate communication layer module.", e);
        // }
        //
        // /* After some indirection we get the injector. */
        // injector = Guice.createInjector(module);

        if (!new File(appsDir).exists()) {
            if (!new File(appsDir).mkdirs()) {
                logger.error("Cannot create apps directory [{}]", appsDir);
            }
        }
        File[] s4rFiles = new File(appsDir).listFiles(new PatternFilenameFilter("\\w+\\.s4r"));
        for (File s4rFile : s4rFiles) {
            loadApp(s4rFile);
        }

        /* Now init + start apps. TODO: implement dynamic loading/unloading using ZK. */
        for (Map.Entry<String, App> appEntry : apps.entrySet()) {
            logger.info("Initializing app " + appEntry.getValue().getClass().getName());
            appEntry.getValue().init();
        }

        for (Map.Entry<String, App> appEntry : apps.entrySet()) {
            logger.info("Initializing app streams " + appEntry.getValue().getClass().getName());
            updateStreams(appEntry.getValue(), appEntry.getKey());
        }

        for (Map.Entry<String, App> appEntry : apps.entrySet()) {
            logger.info("Starting app " + appEntry.getKey() + "/" + appEntry.getValue().getClass().getName());
            appEntry.getValue().start();
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
        logger.info("Local app deployment: using s4r file name [{}] as application name",
                s4r.getName().substring(0, s4r.getName().indexOf(".s4r")));
        return loadApp(s4r, s4r.getName().substring(0, s4r.getName().indexOf(".s4r")));
    }

    public void updateStreams(App app, String appName) {
        // register streams
        List<Streamable<Event>> appStreams = app.getStreams();
        for (Streamable<Event> streamable : appStreams) {
            if (streams.containsKey(streamable.getName())) {
                logger.error("Application {} defines the stream {} but there is already a stream with that name",
                        new String[] { appName, streamable.getName() });
            } else {
                // zkClient.createEphemeral("/" + clusterName + "/streams/producers/" + appName);
                logger.debug("Adding stream {} for app {}");
                streams.put(streamable.getName(), streamable);
                if (eventSources.containsKey(streamable.getName())) {
                    logger.debug("Connecting matching event source for stream {} for app {}", streamable.getName(),
                            appName);
                    eventSources.get(streamable.getName()).subscribeStream(streams.get(streamable.getName()));
                }
            }
        }

        List<EventSource> appEventSources = app.getEventSources();
        for (EventSource eventSource : appEventSources) {
            if (eventSources.containsKey(eventSource.getName())) {
                logger.error(
                        "Application {} defines the event source {} but there is already an event source with that name, from app {}",
                        new String[] { appName, eventSource.getName(),
                                String.valueOf(streams.get(eventSource.getName())) });
            } else {
                // zkClient.createEphemeral("/" + clusterName + "/streams/consumers/" + appName);
                logger.debug("adding event source {} from app {}", eventSource.getName(), appName);
                eventSources.put(eventSource.getName(), eventSource);
            }
            if (streams.containsKey(eventSource.getName())) {
                logger.debug("Connecting matching stream from app {} to event source {}", appName,
                        eventSource.getName());
                eventSource.subscribeStream(streams.get(eventSource.getName()));
            }
        }

    }

    public App loadApp(File s4r, String appName) {

        // TODO handle application upgrade

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
                injector.injectMembers(app);
            } catch (Exception e) {
                logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
                return null;
            }

            App previous = apps.put(appName, app);
            logger.info("Loaded application from file {}", s4r.getAbsolutePath());
            signalOneAppLoaded.countDown();
            return app;
        } catch (IOException e) {
            logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
            return null;
        }

    }

    public void startApp(App app, String appName, String clusterName) {

        app.init();

        updateStreams(app, appName);

        app.start();
    }
}
