package org.apache.s4.tools.helix;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.IdealState;
import org.apache.s4.base.Destination;
import org.apache.s4.base.Event;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ClusterFromHelix;
import org.apache.s4.tools.S4ArgsBase;
import org.apache.s4.tools.Tools;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class GenericEventAdapter {

    public static void main(String[] args) {
        AdapterArgs adapterArgs = new AdapterArgs();

        Tools.parseArgs(adapterArgs, args);
        try {
            String instanceName = "adapter";
            HelixManager manager = HelixManagerFactory.getZKHelixManager(adapterArgs.clusterName, instanceName,
                    InstanceType.SPECTATOR, adapterArgs.zkConnectionString);
            ClusterFromHelix cluster = new ClusterFromHelix(adapterArgs.clusterName, adapterArgs.zkConnectionString,
                    30, 60);
            manager.connect();
            manager.addExternalViewChangeListener(cluster);	

            HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
            Builder keyBuilder = helixDataAccessor.keyBuilder();
            IdealState idealstate = helixDataAccessor.getProperty(keyBuilder.idealStates(adapterArgs.streamName));
            TCPEmitter emitter = new TCPEmitter(cluster, 1000, 1000);
            while (true) {
                int partitionId = ((int) (Math.random() * 1000)) % idealstate.getNumPartitions();
                Event event = new Event();
                event.put("name", String.class, "Hello world to partition:" + partitionId);
                event.setStreamId("names");
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                KryoSerDeser serializer = new KryoSerDeser(classLoader);
//                EventMessage message = new EventMessage("-1", adapterArgs.streamName, serializer.serialize(event));
                System.out.println("Sending event to partition:" + partitionId);
                Destination destination = cluster.getDestination(adapterArgs.streamName, partitionId, emitter.getType());
                emitter.send(destination, serializer.serialize(event));
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Parameters(commandNames = "newStreamProcessor", separators = "=", commandDescription = "Create a new stream processor")
    static class AdapterArgs extends S4ArgsBase {

        @Parameter(names = "-zk", description = "ZooKeeper connection string")
        String zkConnectionString = "localhost:2181";

        @Parameter(names = { "-c", "-cluster" }, description = "Logical name of the S4 cluster", required = true)
        String clusterName;

        @Parameter(names = { "-s", "-streamName" }, description = "Stream Name where the event will be sent to", required = true)
        String streamName;

    }

}
