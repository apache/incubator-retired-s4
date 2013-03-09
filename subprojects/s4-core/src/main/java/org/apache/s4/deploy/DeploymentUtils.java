package org.apache.s4.deploy;

import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for facilitating deployment and configuration operations.
 */
public class DeploymentUtils {

    private static Logger logger = LoggerFactory.getLogger(DeploymentUtils.class);

    /**
     * Uploads an application configuration to the cluster manager
     * 
     * @param appConfig
     *            application configuration
     * @param clusterName
     *            name of the S4 cluster
     * @param deleteIfExists
     *            deletes previous configuration if it existed
     * @param zkString
     *            ZooKeeper connection string (connection to the cluster manager)
     */
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

}
