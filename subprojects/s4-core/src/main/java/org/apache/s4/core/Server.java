package org.apache.s4.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes.Name;
import java.util.jar.JarFile;

import org.apache.s4.base.util.S4RLoader;
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
 * The Server instance coordinates activities in a cluster node including
 * loading and unloading of applications and instantiating the communication
 * layer.
 */
public class Server {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    final private String commModuleName;
    final private String logLevel;
    public static final String MANIFEST_S4_APP_CLASS = "S4-App-Class";
    // NOTE: currently we use a directory, but this will be changed by a URL (ref to zookeeper?), 
    // so that applications can be downloaded from a remote repository
    final private static String S4_APPS_PATH = System.getProperty("s4.apps.path", System.getProperty("user.dir")
            + "/bin/apps");
    List<App> apps = new ArrayList<App>();

    /**
     * 
     */
    @Inject
    public Server(@Named("comm.module") String commModuleName, @Named("s4.logger_level") String logLevel) {
        this.commModuleName = commModuleName;
        this.logLevel = logLevel;
    }

    public void start() throws Exception {

        /* Set up logger basic configuration. */
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.toLevel(logLevel));

        Injector injector;
        AbstractModule module = null;

        /* Initialize communication layer module. */
        try {
            module = (AbstractModule) Class.forName(commModuleName).newInstance();
        } catch (Exception e) {
            logger.error("Unable to instantiate communication layer module.", e);
        }

        /* After some indirection we get the injector. */
        injector = Guice.createInjector(module);

        File[] s4rFiles = new File(S4_APPS_PATH).listFiles(new PatternFilenameFilter("\\w+\\.s4r"));
        for (File s4rFile : s4rFiles) {
            loadApp(injector, s4rFile);
        }

        // now init + start apps
        for (App app : apps) {
            logger.info("Starting app " + app.getClass().getName());
            app.init();
            app.start();
        }

        logger.info("Completed applications startup");

    }

    private void loadApp(Injector injector, File s4r) {

        S4RLoader cl = new S4RLoader(s4r.getAbsolutePath());
        try {
            JarFile s4rFile = new JarFile(s4r);
            if (s4rFile.getManifest() == null) {
                logger.warn("Cannot load s4r archive [{}] : missing manifest file");
                return;
            }
            if (!s4rFile.getManifest().getMainAttributes().containsKey(new Name(MANIFEST_S4_APP_CLASS))) {
                logger.warn("Cannot load s4r archive [{}] : missing attribute [{}] in manifest", s4r.getAbsolutePath(),
                        MANIFEST_S4_APP_CLASS);
                return;
            }
            String appClassName = s4rFile.getManifest().getMainAttributes().getValue(MANIFEST_S4_APP_CLASS);
            App app = null;

            try {
                Object o = (cl.loadClass(appClassName)).newInstance();
                app = (App) o;
            } catch (Exception e) {
                logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
                return;
            }

            Sender sender = injector.getInstance(Sender.class);
            Receiver receiver = injector.getInstance(Receiver.class);
            app.setCommLayer(sender, receiver);
            apps.add(app);
        } catch (IOException e) {
            logger.error("Could not load s4 application form s4r file [{" + s4r.getAbsolutePath() + "}]", e);
        }

    }
}
