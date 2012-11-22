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
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;

/**
 * Used for defining and dimensioning logical clusters in Zookeeper.
 * 
 */
public class TaskSetup
{

  private ZkClient zkclient;
  private boolean isHelixEnabled = true;
  private HelixAdmin helixAdmin;
  org.apache.helix.manager.zk.ZkClient helixZkClient;

  public TaskSetup(String zookeeperAddress)
  {
    if (isHelixEnabled)
    {
      helixZkClient = new org.apache.helix.manager.zk.ZkClient(zookeeperAddress);
      helixZkClient
          .setZkSerializer(new org.apache.helix.manager.zk.ZNRecordSerializer());
      if (!helixZkClient.waitUntilConnected(10, TimeUnit.SECONDS))
      {
        throw new RuntimeException(
            "Could not connect to ZooKeeper after 10 seconds.");
      }
      helixAdmin = new ZKHelixAdmin(helixZkClient);
    } else
    {
      zkclient = new ZkClient(zookeeperAddress);
      zkclient.setZkSerializer(new ZNRecordSerializer());
      if (!zkclient.waitUntilConnected(10, TimeUnit.SECONDS))
      {
        throw new RuntimeException(
            "Could not connect to ZooKeeper after 10 seconds.");
      }
    }
  }

  public void clean(String clusterName)
  {
    if (isHelixEnabled)
    {
      helixAdmin.dropCluster(clusterName);
    } else
    {
      zkclient.deleteRecursive("/s4/clusters/" + clusterName);
    }
  }

  public void setup(String cluster, int tasks, int initialPort)
  {
    if (isHelixEnabled)
    {
      helixAdmin.addCluster(cluster, false);
      StateModelDefinition onlineofflinemodel = new StateModelDefinition(
          new StateModelConfigGenerator().generateConfigForOnlineOffline());
      StateModelDefinition leaderstandbymodel = new StateModelDefinition(
          new StateModelConfigGenerator().generateConfigForLeaderStandby());

      helixAdmin.addStateModelDef(cluster, "OnlineOffline", onlineofflinemodel);
      helixAdmin.addStateModelDef(cluster, "LeaderStandby", leaderstandbymodel);
      
      for (int i = 0; i < tasks; i++)
      {
        InstanceConfig instanceConfig = new InstanceConfig("localhost_"
            + initialPort);
        instanceConfig.setHostName("localhost");
        instanceConfig.setPort("" + initialPort);
        helixAdmin.addInstance(cluster, instanceConfig);
        initialPort = initialPort + 1;
      }

      return;
    }
    try
    {
      zkclient.createPersistent("/s4/streams", true);
    } catch (ZkException ignored)
    {
      // ignore if exists
    }

    zkclient.createPersistent("/s4/clusters/" + cluster + "/tasks", true);
    zkclient.createPersistent("/s4/clusters/" + cluster + "/process", true);
    zkclient.createPersistent("/s4/clusters/" + cluster + "/app", true);
    for (int i = 0; i < tasks; i++)
    {
      String taskId = "Task-" + i;
      ZNRecord record = new ZNRecord(taskId);
      record.putSimpleField("taskId", taskId);
      record.putSimpleField("port", String.valueOf(initialPort + i));
      record.putSimpleField("partition", String.valueOf(i));
      record.putSimpleField("cluster", cluster);
      zkclient.createPersistent("/s4/clusters/" + cluster + "/tasks/" + taskId,
          record);
    }
  }

  public void disconnect()
  {
    if (isHelixEnabled)
    {
      helixZkClient.close();
    } else
    {
      zkclient.close();
    }
  }

}
