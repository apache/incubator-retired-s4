package org.apache.s4.core;

import java.io.InputStream;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.HelixBasedCommModule;
import org.apache.s4.comm.helix.TaskStateModelFactory;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.util.ArchiveFetchException;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.core.util.ParametersInjectionModule;
import org.apache.s4.deploy.AppStateModelFactory;
import org.apache.s4.deploy.DeploymentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import com.google.inject.util.Modules.OverriddenModuleBuilder;

/**
 * This is the bootstrap for S4 nodes.
 * <p>
 * Its roles are to:
 * <ul>
 * <li>register within the S4 cluster (and acquire a partition).
 * <li>look for application deployed on the S4 cluster
 * </ul>
 * <p>
 * When an application is available, custom modules are fetched if necessary and a full-featured S4 node is started. The
 * application code is then downloaded and the app started.
 * <p>
 * For testing purposes, it is also possible to directly start an application without fetching remote code, provided the
 * application classes are available in the classpath.
 * 
 * 
 * 
 */
public class S4HelixBootstrap implements Bootstrap {
    private static Logger logger = LoggerFactory.getLogger(S4HelixBootstrap.class);

    private final AtomicBoolean deployed = new AtomicBoolean(false);

    private final ArchiveFetcher fetcher;

    // private Injector parentInjector;

    CountDownLatch signalOneAppLoaded = new CountDownLatch(1);

    private final String clusterName;

    private final String instanceName;

    private final String zookeeperAddress;
    private final TaskStateModelFactory taskStateModelFactory;

    private final AppStateModelFactory appStateModelFactory;

    private final Cluster cluster;

    private final Lock startingNode = new ReentrantLock();

    public static Injector rootInjector;

    @Inject
    public S4HelixBootstrap(@Named("s4.cluster.name") String clusterName,
            @Named("s4.instance.name") String instanceName, @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout,
            AppStateModelFactory appStateModelFactory, TaskStateModelFactory taskStateModelFactory,
            ArchiveFetcher fetcher, Cluster cluster) {
        this.clusterName = clusterName;
        this.instanceName = instanceName;
        this.zookeeperAddress = zookeeperAddress;
        this.taskStateModelFactory = taskStateModelFactory;
        this.appStateModelFactory = appStateModelFactory;
        this.fetcher = fetcher;
        this.cluster = cluster;
    }

    @Override
    public void start(Injector parentInjector) throws InterruptedException, ArchiveFetchException, UnknownHostException {

        // start a HelixController to manage the cluster
        // TODO set this as optional (small clusters only)
        String controllerName = Inet4Address.getLocalHost().getCanonicalHostName() + UUID.randomUUID().toString();
        HelixControllerMain.startHelixController(zookeeperAddress, clusterName, controllerName,
                HelixControllerMain.STANDALONE);
        // this.parentInjector = parentInjector;
        S4HelixBootstrap.rootInjector = parentInjector;
        registerWithHelix();

        signalOneAppLoaded.await();
    }

    private void registerWithHelix() {
        HelixManager helixManager;
        try {
            helixManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
                    zookeeperAddress);
            helixManager.getStateMachineEngine().registerStateModelFactory("LeaderStandby", taskStateModelFactory);
            helixManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline", appStateModelFactory);
            helixManager.connect();
            helixManager.addExternalViewChangeListener((RoutingTableProvider) cluster);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    public static void startS4App(AppConfig appConfig, Injector parentInjector, ClassLoader modulesLoader) {
        try {
            Injector injector;
            InputStream commConfigFileInputStream = Resources.getResource("default.s4.comm.properties").openStream();
            InputStream coreConfigFileInputStream = Resources.getResource("default.s4.core.properties").openStream();

            logger.info("Initializing S4 app with : {}", appConfig.toString());

            AbstractModule commModule = new HelixBasedCommModule(commConfigFileInputStream);
            AbstractModule coreModule = new HelixBasedCoreModule(coreConfigFileInputStream);

            List<com.google.inject.Module> extraModules = new ArrayList<com.google.inject.Module>();
            for (String moduleClass : appConfig.getCustomModulesNames()) {
                extraModules.add((Module) Class.forName(moduleClass, true, modulesLoader).newInstance());
            }
            Module combinedModule = Modules.combine(commModule, coreModule);
            if (extraModules.size() > 0) {
                OverriddenModuleBuilder overridenModuleBuilder = Modules.override(combinedModule);
                combinedModule = overridenModuleBuilder.with(extraModules);
            }

            if (appConfig.getNamedParameters() != null && !appConfig.getNamedParameters().isEmpty()) {

                logger.debug("Adding named parameters for injection : {}", appConfig.getNamedParametersAsString());
                Map<String, String> namedParameters = new HashMap<String, String>();

                namedParameters.putAll(appConfig.getNamedParameters());
                combinedModule = Modules.override(combinedModule).with(new ParametersInjectionModule(namedParameters));
            }

            if (appConfig.getAppClassName() != null && Strings.isNullOrEmpty(appConfig.getAppURI())) {
                // In that case we won't be using an S4R classloader, app classes are available from the current
                // classloader
                // The app module provides bindings specific to the app class loader, in this case the current thread's
                // class loader.
                AppModule appModule = new AppModule(Thread.currentThread().getContextClassLoader());
                // NOTE: because the app module can be overriden
                combinedModule = Modules.override(appModule).with(combinedModule);
                injector = parentInjector.createChildInjector(combinedModule);
                logger.info("Starting S4 app with application class [{}]", appConfig.getAppClassName());
                App app = (App) injector.getInstance(Class.forName(appConfig.getAppClassName(), true, modulesLoader));
                app.init();
                app.start();
            } else {
                injector = parentInjector.createChildInjector(combinedModule);
                if (Strings.isNullOrEmpty(appConfig.getAppURI())) {
                    logger.info("S4 node in standby until app class or app URI is specified");
                }
                Server server = injector.getInstance(Server.class);
                server.setInjector(injector);
                DeploymentManager deploymentManager = injector.getInstance(DeploymentManager.class);
                deploymentManager.deploy(appConfig);
                // server.start(injector);
            }
        } catch (Exception e) {
            logger.error("Cannot start S4 node", e);
            System.exit(1);
        }
    }
}
