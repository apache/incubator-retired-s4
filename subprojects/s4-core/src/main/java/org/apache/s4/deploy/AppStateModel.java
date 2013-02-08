package org.apache.s4.deploy;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.s4.base.util.ModulesLoader;
import org.apache.s4.comm.ModulesLoaderFactory;
import org.apache.s4.comm.util.ArchiveFetchException;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.S4Bootstrap;
import org.apache.s4.core.S4HelixBootstrap;
import org.apache.s4.core.util.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Injector;

@StateModelInfo(states = { "ONLINE,OFFLINE" }, initialState = "OFFLINE")
public class AppStateModel extends StateModel {
    private static Logger logger = LoggerFactory.getLogger(AppStateModel.class);
    private final String appName;
    private final ArchiveFetcher fetcher;

    public AppStateModel(String appName, ArchiveFetcher fetcher) {
        this.appName = appName;
        this.fetcher = fetcher;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void deploy(Message message, NotificationContext context) throws Exception {
        logger.info("Deploying app:" + appName);
        HelixManager manager = context.getManager();
        ConfigAccessor configAccessor = manager.getConfigAccessor();
        AppConfig appConfig = createAppConfig(manager, configAccessor);
        loadModulesAndStartNode(S4HelixBootstrap.rootInjector, appConfig);
        logger.info("Deployed app:" + appName);

    }

    private AppConfig createAppConfig(HelixManager manager, ConfigAccessor configAccessor) {
        ConfigScopeBuilder builder = new ConfigScopeBuilder();
        ConfigScope scope = builder.forCluster(manager.getClusterName()).forResource(appName).build();
        AppConfig appConfig = new AppConfig.Builder()
                .appClassName(configAccessor.get(scope, AppConfig.APP_CLASS))
                .appName(configAccessor.get(scope, AppConfig.APP_NAME))
                .customModulesNames(
                        getListFromCommaSeparatedValues(configAccessor.get(scope, AppConfig.MODULES_CLASSES)))
                .customModulesURIs(getListFromCommaSeparatedValues(configAccessor.get(scope, AppConfig.MODULES_URIS)))
                .appURI(configAccessor.get(scope, AppConfig.APP_URI)).build();

        return appConfig;
    }

    private static List<String> getListFromCommaSeparatedValues(String values) {
        if (com.google.common.base.Strings.isNullOrEmpty(values)) {
            return Collections.emptyList();
        }
        return Arrays.asList(values.split("[,]"));

    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void undeploy(Message message, NotificationContext context) throws Exception {
        logger.info("Undeploying app:" + appName);
        HelixManager manager = context.getManager();
        ConfigAccessor configAccessor = manager.getConfigAccessor();
        AppConfig config = createAppConfig(manager, configAccessor);
        // deploymentManager.undeploy(config);
        logger.info("Undeploying app:" + appName);

    }

    private void loadModulesAndStartNode(final Injector parentInjector, final AppConfig appConfig)
            throws ArchiveFetchException {

        String appName = appConfig.getAppName();

        List<File> modulesLocalCopies = new ArrayList<File>();

        for (String uriString : appConfig.getCustomModulesURIs()) {
            modulesLocalCopies.add(S4Bootstrap.fetchModuleAndCopyToLocalFile(appName, uriString, fetcher));
        }
        final ModulesLoader modulesLoader = new ModulesLoaderFactory().createModulesLoader(modulesLocalCopies);

        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                // load app class through modules classloader and start it
                S4Bootstrap.startS4App(appConfig, parentInjector, modulesLoader);
                // signalOneAppLoaded.countDown();
            }
        }, "S4 platform loader");
        t.start();

    }

}
