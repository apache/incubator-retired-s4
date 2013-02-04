package org.apache.s4.deploy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.App;
import org.apache.s4.core.Server;
import org.apache.s4.core.util.AppConfig;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;

public class DeploymentUtils {

    private static Logger logger = LoggerFactory.getLogger(DeploymentUtils.class);

    public static void initAppConfig(AppConfig appConfig, String clusterName, boolean deleteIfExists, String zkString) {
        ZkClient zk = new ZkClient(zkString);
        ZkSerializer serializer = new ZNRecordSerializer();
        zk.setZkSerializer(serializer);

        if (zk.exists("/s4/clusters/" + clusterName + "/app/s4App")) {
            if (deleteIfExists) {
                zk.delete("/s4/clusters/" + clusterName + "/app/s4App");
            }
        }
        try {
            zk.create("/s4/clusters/" + clusterName + "/app/s4App", appConfig.asZNRecord("app"), CreateMode.PERSISTENT);
        } catch (ZkNodeExistsException e) {
            if (!deleteIfExists) {
                logger.warn("Node {} already exists, will not overwrite", "/s4/clusters/" + clusterName + "/app/s4App");
            } else {
                throw new RuntimeException("Node should have been deleted");
            }
        }
        zk.close();
    }
	public static void deploy(Server server, ArchiveFetcher fetcher, String clusterName, AppConfig appConfig) throws DeploymentFailedException {
        if (appConfig.getAppURI() == null) {
            if (appConfig.getAppClassName() != null) {
                try {
                    App app = (App) DeploymentUtils.class.getClassLoader().loadClass(appConfig.getAppClassName()).newInstance();
                    server.startApp(app, "appName", clusterName);
                } catch (Exception e) {
                    logger.error("Cannot start application: cannot instantiate app class {} due to: {}",
                            appConfig.getAppClassName(), e.getMessage());
                    return;
                }
            }
            logger.info("{} value not set for {} : no application code will be downloaded", DeploymentManager.S4R_URI, appConfig.getAppName());
            return;
        }
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
                throw new DeploymentFailedException("Cannot deploy application [" + appConfig.getAppName() + "]", e1);
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
            App loaded = server.loadApp(localS4RFileCopy, appConfig.getAppName());
            if (loaded != null) {
                logger.info("Successfully installed application {}", appConfig.getAppName());
                // TODO sync with other nodes? (e.g. wait for other apps deployed before starting?
                server.startApp(loaded, appConfig.getAppName(), clusterName);
            } else {
                throw new DeploymentFailedException("Cannot deploy application [" + appConfig.getAppName()
                        + "] from URI [" + uri.toString() + "] : cannot start application");
            }

        } catch (URISyntaxException e) {
            logger.error("Cannot deploy app {} : invalid uri for fetching s4r archive {} : {} ", new String[] {
                    appConfig.getAppName(), appConfig.getAppURI(), e.getMessage() });
            throw new DeploymentFailedException("Cannot deploy application [" + appConfig.getAppName() + "]", e);
        }
	}
}
