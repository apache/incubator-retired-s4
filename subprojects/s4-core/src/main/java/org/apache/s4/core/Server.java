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
import java.util.concurrent.CountDownLatch;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.util.S4RLoader;
import org.apache.s4.base.util.S4RLoaderFactory;
import org.apache.s4.comm.topology.AssignmentFromZK;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.deploy.DeploymentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

/**
 * The Server instance coordinates activities in a cluster node including loading and unloading of applications and
 * instantiating the communication layer.
 */
public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final String logLevel;
    public static final String MANIFEST_S4_APP_CLASS = "S4-App-Class";
    CountDownLatch signalOneAppLoaded = new CountDownLatch(1);

    private Injector injector;

    @Inject
    private DeploymentManager deploymentManager;

    @Inject
    private AssignmentFromZK assignment;

    private final ZkClient zkClient;

    /**
     *
     */
    @Inject
    public Server(String commModuleName, @Named("s4.logger_level") String logLevel,
            @Named("s4.cluster.name") String clusterName, @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout) {
        this.logLevel = logLevel;

        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        zkClient.setZkSerializer(new ZNRecordSerializer());
    }

    public void start(Injector injector) throws Exception {

        this.injector = injector;
        /* Set up logger basic configuration. */
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(logLevel));

        if (deploymentManager != null) {
            deploymentManager.start();
        }

        // wait for an app to be loaded (otherwise the server would not have anything to do and just die)
        signalOneAppLoaded.await();

    }

    public App loadApp(File s4r, String appName) {

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

    public void startApp(App app, String appName, String clusterName) {

        app.init();

        app.start();
    }
}
