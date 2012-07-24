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

package org.apache.s4.comm.tools;

import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;

/**
 * Used for defining and dimensioning logical clusters in Zookeeper.
 *
 */
public class TaskSetup {

    private ZkClient zkclient;

    public TaskSetup(String zookeeperAddress) {
        zkclient = new ZkClient(zookeeperAddress);
        zkclient.setZkSerializer(new ZNRecordSerializer());
        if (!zkclient.waitUntilConnected(10, TimeUnit.SECONDS)) {
            throw new RuntimeException("Could not connect to ZooKeeper after 10 seconds.");
        }
    }

    public void clean(String clusterName, String topologyName) {
        zkclient.deleteRecursive("/s4/clusters/" + topologyName);
    }

    public void clean(String clusterName) {
        zkclient.deleteRecursive("/" + clusterName);
    }

    public void setup(String cluster, int tasks, int initialPort) {
        try {
            zkclient.createPersistent("/s4/streams", true);
        } catch (ZkException ignored) {
            // ignore if exists
        }

        zkclient.createPersistent("/s4/clusters/" + cluster + "/tasks", true);
        zkclient.createPersistent("/s4/clusters/" + cluster + "/process", true);
        zkclient.createPersistent("/s4/clusters/" + cluster + "/app", true);
        for (int i = 0; i < tasks; i++) {
            String taskId = "Task-" + i;
            ZNRecord record = new ZNRecord(taskId);
            record.putSimpleField("taskId", taskId);
            record.putSimpleField("port", String.valueOf(initialPort + i));
            record.putSimpleField("partition", String.valueOf(i));
            record.putSimpleField("cluster", cluster);
            zkclient.createPersistent("/s4/clusters/" + cluster + "/tasks/" + taskId, record);
        }
    }

    public void disconnect() {
        zkclient.close();
    }

}
