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

package org.apache.s4.deploy;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.Server;
import org.apache.s4.core.util.AppConfig;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * 
 * <p>
 * Monitors application availability on a given s4 cluster. Starts the application when available.
 * </p>
 * 
 * <p>
 * More specifically, this class observes the children of <code>/&lt;s4-cluster-name&gt;/apps</code>. Children
 * correspond to S4 applications. A child's metadata contains a URI that refers to the s4r file that contains the s4
 * application code.
 * </p>
 * 
 * <p>
 * At startup, existing applications are loaded by, for each detected application:
 * <ol>
 * <li>reading the associated URI
 * <li>fetching the s4r archive from that URI, through the protocol specified in the URI, and copying the s4r to a local
 * directory. Protocol handlers are not currently pluggable and must be implemented in this class.
 * <li>loading and starting the application
 * </ol>
 * 
 * <p>
 * Then, whenever new app children are detected, the deployment manager re-executes the above steps for those new
 * applications
 * </p>
 */
public class DistributedDeploymentManager implements DeploymentManager {

    private static Logger logger = LoggerFactory.getLogger(DistributedDeploymentManager.class);

    private final String clusterName;

    private final ZkClient zkClient;
    private final String appPath;
    private final Server server;
    boolean deployed = false;

    private final ArchiveFetcher fetcher;

    @Inject
    public DistributedDeploymentManager(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout, Server server, ArchiveFetcher fetcher) {

        this.clusterName = clusterName;
        this.server = server;
        this.fetcher = fetcher;

        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        String appDir = "/s4/clusters/" + clusterName + "/app";
        if (!zkClient.exists(appDir)) {
            zkClient.create(appDir, null, CreateMode.PERSISTENT);
        }
        appPath = appDir + "/s4App";
        zkClient.subscribeDataChanges(appPath, new AppChangeListener());
    }

    public void deployApplication() throws DeploymentFailedException {
        ZNRecord appData = zkClient.readData(appPath);
        AppConfig appConfig = new AppConfig(appData);
    }

    // NOTE: in theory, we could support any protocol by implementing a chained visitor scheme,
    // but that's probably not that useful, and we can simply provide whichever protocol is needed

    private final class AppChangeListener implements IZkDataListener {
        @Override
        public void handleDataDeleted(String dataPath) throws Exception {
            logger.error("Application undeployment is not supported yet");
        }

        @Override
        public void handleDataChange(String dataPath, Object data) throws Exception {
            if (!deployed) {
                deployApplication();
            } else {
                logger.error("There is already an application deployed on this S4 node");
            }

        }

    }

    @Override
    public void start() {
        if (zkClient.exists(appPath)) {
            try {
                deployApplication();
            } catch (DeploymentFailedException e) {
                logger.error("Cannot deploy application", e);
            }
        }
    }

    @Override
    public void deploy(AppConfig appConfig) throws DeploymentFailedException {
        DeploymentUtils.deploy(server, fetcher, clusterName, appConfig);
        deployed = true;
    }

    @Override
    public void undeploy(AppConfig appConfig) {
        // TODO Auto-generated method stub

    }
}
