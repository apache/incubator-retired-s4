package org.apache.s4.comm;

import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.s4.comm.helix.TaskStateModelFactory;
import org.apache.s4.comm.topology.Clusters;
import org.apache.s4.comm.topology.ClustersFromHelix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class HelixBasedCommModule extends AbstractModule {

    private static Logger logger = LoggerFactory.getLogger(DefaultCommModule.class);

    /**
     * 
     * @param commConfigInputStream
     *            input stream from a configuration file
     * @param clusterName
     *            the name of the cluster to which the current node belongs. If specified in the configuration file,
     *            this parameter will be ignored.
     */
    public HelixBasedCommModule() {
    }

    @Override
    protected void configure() {

        // a node holds a single partition assignment
        // ==> Assignment and Cluster are singletons so they can be shared between comm layer and app.
        bind(StateModelFactory.class).annotatedWith(Names.named("s4.task.statemodelfactory")).to(
                TaskStateModelFactory.class);

        bind(Clusters.class).to(ClustersFromHelix.class);

    }

}
