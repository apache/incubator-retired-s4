package org.apache.s4.deploy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.ConfigScope;
import org.apache.helix.ConfigScopeBuilder;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.s4.core.App;
import org.apache.s4.core.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

@StateModelInfo(states = { "ONLINE,OFFLINE" }, initialState = "OFFLINE")
public class AppStateModel extends StateModel {
    private static Logger logger = LoggerFactory.getLogger(AppStateModel.class);
    private final String appName;
    private final Server server;

    public AppStateModel(Server server, String appName) {
        this.server = server;
        this.appName = appName;
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void deploy(Message message, NotificationContext context) throws Exception {
        HelixManager manager = context.getManager();
        ConfigAccessor configAccessor = manager.getConfigAccessor();
        ConfigScopeBuilder builder = new ConfigScopeBuilder();
        ConfigScope scope = builder.forCluster(manager.getClusterName()).forResource(appName).build();
        String uriString = configAccessor.get(scope, DistributedDeploymentManager.S4R_URI);
        String clusterName = manager.getClusterName();
        try {
            URI uri = new URI(uriString);
            // fetch application
            File localS4RFileCopy;
            try {
                localS4RFileCopy = File.createTempFile("tmp", "s4r");
            } catch (IOException e1) {
                logger.error(
                        "Cannot deploy app [{}] because a local copy of the S4R file could not be initialized due to [{}]",
                        appName, e1.getClass().getName() + "->" + e1.getMessage());
                throw new DeploymentFailedException("Cannot deploy application [" + appName + "]", e1);
            }
            localS4RFileCopy.deleteOnExit();
            try {
                if (ByteStreams.copy(DistributedDeploymentManager.fetchS4App(uri),
                        Files.newOutputStreamSupplier(localS4RFileCopy)) == 0) {
                    throw new DeploymentFailedException("Cannot copy archive from [" + uri.toString() + "] to ["
                            + localS4RFileCopy.getAbsolutePath() + "] (nothing was copied)");
                }
            } catch (IOException e) {
                throw new DeploymentFailedException("Cannot deploy application [" + appName + "] from URI ["
                        + uri.toString() + "] ", e);
            }
            // install locally
            App loaded = server.loadApp(localS4RFileCopy, appName);
            if (loaded != null) {
                logger.info("Successfully installed application {}", appName);
                // TODO sync with other nodes? (e.g. wait for other apps deployed before
                // starting?
                server.startApp(loaded, appName, clusterName);
            } else {
                throw new DeploymentFailedException("Cannot deploy application [" + appName + "] from URI ["
                        + uri.toString() + "] : cannot start application");
            }

        } catch (URISyntaxException e) {
            logger.error("Cannot deploy app {} : invalid uri for fetching s4r archive {} : {} ", new String[] {
                    appName, uriString, e.getMessage() });
            throw new DeploymentFailedException("Cannot deploy application [" + appName + "]", e);
        }
    }

    @Transition(from = "OFFLINE", to = "ONLINE")
    public void undeploy(Message message, NotificationContext context) throws Exception {
        // todo
    }

}
