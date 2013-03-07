package org.apache.s4.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.s4.base.util.ModulesLoader;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.ModulesLoaderFactory;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.comm.util.ArchiveFetchException;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.core.util.ParametersInjectionModule;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
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
public class S4Bootstrap {
    private static Logger logger = LoggerFactory.getLogger(S4Bootstrap.class);

    private final ZkClient zkClient;
    private final String appPath;
    private final AtomicBoolean deployed = new AtomicBoolean(false);

    private final ArchiveFetcher fetcher;

    private Injector parentInjector;

    CountDownLatch signalOneAppLoaded = new CountDownLatch(1);

    @Inject
    public S4Bootstrap(@Named("s4.cluster.name") String clusterName, ZkClient zkClient, ArchiveFetcher fetcher) {

        this.fetcher = fetcher;
        this.zkClient = zkClient;

        String appDir = "/s4/clusters/" + clusterName + "/app";
        if (!zkClient.exists(appDir)) {
            zkClient.create(appDir, null, CreateMode.PERSISTENT);
        }
        appPath = appDir + "/s4App";
        zkClient.subscribeDataChanges(appPath, new AppChangeListener());
    }

    public void start(Injector parentInjector) throws InterruptedException, ArchiveFetchException {
        this.parentInjector = parentInjector;
        if (zkClient.exists(appPath)) {
            if (!deployed.get()) {
                loadModulesAndStartNode(parentInjector);
            }
        }

        signalOneAppLoaded.await();
    }

    private void loadModulesAndStartNode(final Injector parentInjector) throws ArchiveFetchException {

        final ZNRecord appData = zkClient.readData(appPath);
        // can be null
        final AppConfig appConfig = new AppConfig(appData);

        String appName = appConfig.getAppName();

        List<File> modulesLocalCopies = new ArrayList<File>();

        for (String uriString : appConfig.getCustomModulesURIs()) {
            modulesLocalCopies.add(fetchModuleAndCopyToLocalFile(appName, uriString));
        }
        final ModulesLoader modulesLoader = new ModulesLoaderFactory().createModulesLoader(modulesLocalCopies);

        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                // load app class through modules classloader and start it
                S4Bootstrap.startS4App(appConfig, parentInjector, modulesLoader);
                signalOneAppLoaded.countDown();
            }
        }, "S4 platform loader");
        t.start();

    }

    private File fetchModuleAndCopyToLocalFile(String appName, String uriString) throws ArchiveFetchException {

        URI uri;
        try {
            uri = new URI(uriString);
        } catch (URISyntaxException e2) {
            throw new ArchiveFetchException("Invalid module URI : [" + uriString + "]", e2);
        }
        File localModuleFileCopy;
        try {
            localModuleFileCopy = File.createTempFile("tmp", "module");
        } catch (IOException e1) {
            logger.error(
                    "Cannot deploy app [{}] because a local copy of the module file could not be initialized due to [{}]",
                    appName, e1.getClass().getName() + "->" + e1.getMessage());
            throw new ArchiveFetchException("Cannot deploy application [" + appName + "]", e1);
        }
        localModuleFileCopy.deleteOnExit();
        try {
            if (ByteStreams.copy(fetcher.fetch(uri), Files.newOutputStreamSupplier(localModuleFileCopy)) == 0) {
                throw new ArchiveFetchException("Cannot copy archive from [" + uri.toString() + "] to ["
                        + localModuleFileCopy.getAbsolutePath() + "] (nothing was copied)");
            }
        } catch (Exception e) {
            throw new ArchiveFetchException("Cannot deploy application [" + appName + "] from URI [" + uri.toString()
                    + "] ", e);
        }
        return localModuleFileCopy;
    }

    public static void startS4App(AppConfig appConfig, Injector parentInjector, ClassLoader modulesLoader) {
        try {
            Injector injector;
            InputStream commConfigFileInputStream = Resources.getResource("default.s4.comm.properties").openStream();
            InputStream coreConfigFileInputStream = Resources.getResource("default.s4.core.properties").openStream();

            logger.info("Initializing S4 app with : {}", appConfig.toString());

            AbstractModule commModule = new DefaultCommModule(commConfigFileInputStream);
            AbstractModule coreModule = new DefaultCoreModule(coreConfigFileInputStream);

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
                server.start(injector);
            }
        } catch (Exception e) {
            logger.error("Cannot start S4 node", e);
            System.exit(1);
        }
    }

    class AppChangeListener implements IZkDataListener {

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            if (!deployed.get()) {
                loadModulesAndStartNode(parentInjector);
                deployed.set(true);
            }

        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            logger.error("Application undeployment is not supported yet");
        }
    }
}
