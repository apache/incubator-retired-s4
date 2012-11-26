package org.apache.s4.comm.topology;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.s4.comm.helix.S4StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

@Singleton
public class AssignmentFromHelix implements Assignment
{
  private static final Logger logger = LoggerFactory
      .getLogger(AssignmentFromHelix.class);

  private String clusterName;
  private final String zookeeperAddress;
  private String machineId;
  private HelixManager zkHelixManager;
  
  private HelixDataAccessor helixDataAccessor;
  AtomicReference<ClusterNode> clusterNodeRef;
  private final Lock lock;
  private final AtomicBoolean currentlyOwningTask;
  private final Condition taskAcquired;

  private final StateModelFactory<? extends StateModel> taskStateModelFactory;
  //private final StateModelFactory<? extends StateModel> appStateModelFactory;

  @Inject
  public AssignmentFromHelix(@Named("s4.cluster.name") String clusterName,
                             @Named("s4.instance.name") String instanceName,
                             @Named("s4.cluster.zk_address") String zookeeperAddress 
                             ) throws Exception
  {
    this.taskStateModelFactory = new S4StateModelFactory();
//    this.appStateModelFactory = appStateModelFactory;
    System.out.println("here i am");
    this.clusterName = clusterName;
    this.zookeeperAddress = zookeeperAddress;
    machineId = "localhost";
    lock = new ReentrantLock();
    ZkClient zkClient = new ZkClient(zookeeperAddress);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    zkClient.waitUntilConnected(60, TimeUnit.SECONDS);
    BaseDataAccessor<ZNRecord> baseDataAccessor = new ZkBaseDataAccessor<ZNRecord>(
        zkClient);
    helixDataAccessor = new ZKHelixDataAccessor(clusterName, baseDataAccessor);
    clusterNodeRef = new AtomicReference<ClusterNode>();
    taskAcquired = lock.newCondition();
    currentlyOwningTask = new AtomicBoolean(true);
    try
    {
      machineId = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e)
    {
      logger.warn("Unable to get hostname", e);
      machineId = "UNKNOWN";
    }
    ClusterNode node = new ClusterNode(-1,
        Integer.parseInt(instanceName.split("_")[1]), machineId,
        instanceName);
    clusterNodeRef.set(node);
    currentlyOwningTask.set(true);
  }

  @Inject
  public void init()
  {
    //joinCluster();
  }

  @Override
  public ClusterNode assignClusterNode()
  {
    lock.lock();
    try
    {
      while (!currentlyOwningTask.get())
      {
        taskAcquired.awaitUninterruptibly();
      }
    } catch (Exception e)
    {
      logger.error("Exception while waiting to join the cluster");
      return null;
    } finally
    {
      lock.unlock();
    }
    return clusterNodeRef.get();
  }

  public void joinClusterOld()
  {
    lock.lock();
    try
    {
      Builder keyBuilder = helixDataAccessor.keyBuilder();
      do
      {
        List<InstanceConfig> instances = helixDataAccessor
            .getChildValues(keyBuilder.instanceConfigs());
        List<String> liveInstances = helixDataAccessor.getChildNames(keyBuilder
            .liveInstances());
        for (InstanceConfig instanceConfig : instances)
        {
          String instanceName = instanceConfig.getInstanceName();
          if (!liveInstances.contains(instanceName))
          {
            zkHelixManager = HelixManagerFactory.getZKHelixManager(clusterName,
                instanceName, InstanceType.PARTICIPANT, zookeeperAddress);
            zkHelixManager.getStateMachineEngine().registerStateModelFactory(
                "LeaderStandby", taskStateModelFactory);
          
            zkHelixManager.connect();
            ClusterNode node = new ClusterNode(-1,
                Integer.parseInt(instanceConfig.getPort()), machineId,
                instanceName);
            clusterNodeRef.set(node);
            currentlyOwningTask.set(true);
            taskAcquired.signalAll();
            break;
          }
        }
        if (instances.size() == liveInstances.size())
        {
          System.out
              .println("No more nodes can join the cluster. Will wait for some node to die.");
          Thread.sleep(100000);
        }
      } while (!currentlyOwningTask.get());
      System.out.println("Joined the cluster:"+ clusterName +" as "+ clusterNodeRef.get().getTaskId());
    } catch (Exception e)
    {
      e.printStackTrace();
    } finally
    {
      lock.unlock();
    }
  }

}
