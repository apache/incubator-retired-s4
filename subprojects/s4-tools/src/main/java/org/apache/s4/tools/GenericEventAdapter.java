package org.apache.s4.tools;

import java.util.Collections;
import java.util.List;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.IdealState;
import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.comm.serialize.KryoSerDeser;
import org.apache.s4.comm.tcp.TCPEmitter;
import org.apache.s4.comm.topology.ClusterFromHelix;
import org.apache.s4.tools.DeployApp.DeployAppArgs;

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
            TCPEmitter emitter = new TCPEmitter(cluster, 1000);
            while (true) {
                int partitionId = ((int) (Math.random() * 1000)) % idealstate.getNumPartitions();
                Event event = new Event();
                event.put("name", String.class, "Hello world to partition:" + partitionId);
                KryoSerDeser serializer = new KryoSerDeser();
                EventMessage message = new EventMessage("-1", adapterArgs.streamName, serializer.serialize(event));
                System.out.println("Sending event to partition:" + partitionId);
                emitter.send(partitionId, message);
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
