package org.apache.s4.tools;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ClusterFromHelix;

public class GenericEventAdapter
{
  
  public static void main(String[] args)
  {
    try
    {
      String clusterName = "cluster1";
      String instanceName = "adapter";
      String zkAddr= "localhost:2181";
      HelixManager manager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.SPECTATOR, zkAddr);
      ClusterFromHelix cluster = new ClusterFromHelix("cluster1","localhost:2181",30,60);
      manager.connect();
      manager.addExternalViewChangeListener(cluster);
      
      
      TCPEmitter emitter = new TCPEmitter(cluster, 1000);
      while(true){
        int partitionId = ((int)(Math.random()*1000))%4;
        Event event = new Event();
        event.put("name", String.class, "Hello world to partition:"+ partitionId);
        KryoSerDeser serializer = new KryoSerDeser();
        EventMessage message = new EventMessage("-1", "names", serializer.serialize(event));
        System.out.println("Sending event to partition");
        emitter.send(partitionId, message);
        Thread.sleep(1000);
      }
    } catch (Exception e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
  }

}
