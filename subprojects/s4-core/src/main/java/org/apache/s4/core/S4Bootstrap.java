/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.s4.base.util.ModulesLoader;
import org.apache.s4.base.util.S4RLoader;
import org.apache.s4.base.util.S4RLoaderFactory;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.ModulesLoaderFactory;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.core.util.ArchiveFetchException;
import org.apache.s4.core.util.ArchiveFetcher;
import org.apache.s4.core.util.ParametersInjectionModule;
import org.apache.s4.deploy.DeploymentFailedException;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * <li>wait for an application to be published on the S4 cluster
 * </ul>
 * <p>
 * When an application is available, custom modules are fetched if necessary and a full-featured S4 node is started. The
 * application code is then downloaded and the app started.
 * <p>
 * For testing purposes, it is also possible to start an application without packaging code, provided the application
 * classes are available in the classpath.
 * 
 * 
 * 
 */
public class S4Bootstrap {
    private static Logger logger = LoggerFactory.getLogger(S4Bootstrap.class);
    public static final String MANIFEST_S4_APP_CLASS = "S4-App-Class";
    public static final String S4R_URI = "s4r_uri";

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
                loadModulesAndStartApp(parentInjector);
            }
        }

        signalOneAppLoaded.await();
    }

    private void loadModulesAndStartApp(final Injector parentInjector) throws ArchiveFetchException {

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
                startS4App(appConfig, parentInjector, modulesLoader);
                signalOneAppLoaded.countDown();
            }
        }, "S4 platform loader");
        t.start();

    }

    private void startS4App(AppConfig appConfig, Injector parentInjector, ClassLoader modulesLoader) {
        try {
            App app = loadApp(appConfig, modulesLoader);

            // use correct classLoader for running the app initialization
            Thread.currentThread().setContextClassLoader(app.getClass().getClassLoader());

            app.init();
            app.start();

        } catch (Exception e) {
            logger.error("Cannot start S4 node", e);
            System.exit(1);
        }
    }

    private App loadApp(AppConfig appConfig, ClassLoader modulesLoader) throws DeploymentFailedException {

        Module combinedPlatformModule;
        try {
            combinedPlatformModule = loadPlatformModules(appConfig, modulesLoader);
        } catch (Exception e) {
            throw new DeploymentFailedException("Cannot load platform modules", e);
        }

        if (appConfig.getAppURI() == null) {
            if (appConfig.getAppClassName() != null) {
                try {
                    // In that case we won't be using an S4R classloader, app classes are available from the current
                    // classloader
                    // The app module provides bindings specific to the app class loader, in this case the current
                    // thread's
                    // class loader.
                    AppModule appModule = new AppModule(Thread.currentThread().getContextClassLoader());
                    // NOTE: because the app module can be overriden
                    Module combinedModule = Modules.override(appModule).with(combinedPlatformModule);
                    Injector injector = parentInjector.createChildInjector(combinedModule);
                    logger.info("Starting S4 app with application class [{}]", appConfig.getAppClassName());
                    return (App) injector.getInstance(Class.forName(appConfig.getAppClassName(), true, modulesLoader));

                    // server.startApp(app, "appName", clusterName);
                } catch (Exception e) {
                    throw new DeploymentFailedException(String.format(
                            "Cannot start application: cannot instantiate app class %s due to: %s",
                            appConfig.getAppClassName(), e.getMessage()), e);
                }
            } else {
                throw new DeploymentFailedException(
                        "Application class name must be specified when application URI omitted");
            }
        } else {
            try {
                URI uri = new URI(appConfig.getAppURI());

                // fetch application
                File localS4RFileCopy;
                try {
                    localS4RFileCopy = File.createTempFile("tmp", "s4r");
                } catch (IOException e1) {
                    logger.error(
                            "Cannot deploy app [{}] because a local copy of the S4R file could not be initialized due to [{}]",
                            appConfig.getAppName(), e1.getClass().getName() + "->" + e1.getMessage());
                    throw new DeploymentFailedException("Cannot deploy application [" + appConfig.getAppName() + "]",
                            e1);
                }
                localS4RFileCopy.deleteOnExit();
                try {
                    if (ByteStreams.copy(fetcher.fetch(uri), Files.newOutputStreamSupplier(localS4RFileCopy)) == 0) {
                        throw new DeploymentFailedException("Cannot copy archive from [" + uri.toString() + "] to ["
                                + localS4RFileCopy.getAbsolutePath() + "] (nothing was copied)");
                    }
                } catch (Exception e) {
                    throw new DeploymentFailedException("Cannot deploy application [" + appConfig.getAppName()
                            + "] from URI [" + uri.toString() + "] ", e);
                }
                // install locally
                Injector injector = parentInjector.createChildInjector(combinedPlatformModule);

                App loadedApp = loadS4R(injector, localS4RFileCopy, appConfig.getAppName());
                if (loadedApp != null) {
                    return loadedApp;
                } else {
                    throw new DeploymentFailedException("Cannot deploy application [" + appConfig.getAppName()
                            + "] from URI [" + uri.toString() + "] : cannot start application");
                }

            } catch (URISyntaxException e) {
                throw new DeploymentFailedException(String.format(
                        "Cannot deploy application [%s] : invalid URI for fetching S4R archive %s : %s", new Object[] {
                                appConfig.getAppName(), appConfig.getAppURI(), e.getMessage() }), e);
            }
        }
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

    private static Module loadPlatformModules(AppConfig appConfig, ClassLoader modulesLoader) throws IOException,
            InstantiationException, IllegalAccessException, ClassNotFoundException {
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
        return combinedModule;
    }

    private App loadS4R(Injector injector, File s4r, String appName) {

        // TODO handle application upgrade
        logger.info("Loading application [{}] from file [{}]", appName, s4r.getAbsolutePath());

        S4RLoaderFactory loaderFactory = injector.getInstance(S4RLoaderFactory.class);
        S4RLoader appClassLoader = loaderFactory.createS4RLoader(s4r.getAbsolutePath());
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
                Object o = (appClassLoader.loadClass(appClassName)).newInstance();
                app = (App) o;
                // we use the app module to provide bindings that depend upon a classloader savy of app classes, e.g.
                // for serialization/deserialization
                AppModule appModule = new AppModule(appClassLoader);
                injector.createChildInjector(appModule).injectMembers(app);
            } catch (Exception e) {
                logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
                return null;
            }

            logger.info("Loaded application from file {}", s4r.getAbsolutePath());
            signalOneAppLoaded.countDown();
            return app;
        } catch (IOException e) {
            logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
            return null;
        }

    }

    class AppChangeListener implements IZkDataListener {

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            if (!deployed.get()) {
                loadModulesAndStartApp(parentInjector);
                deployed.set(true);
            }

        }

        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            logger.error("Application undeployment is not supported yet");
        }
    }
}
