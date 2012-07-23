package org.apache.s4.fixtures;

import org.apache.s4.comm.topology.Assignment;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.Clusters;

import com.google.inject.AbstractModule;

/**
 * 
 * Used for injecting non-fail-fast zookeeper client classes.
 * 
 * Here is why:
 * 
 * <ul>
 * <li>tests contained in a single junit class are not forked: forking is on a class basis</li>
 * <li>zookeeper client classes are injected during the tests</li>
 * <li>zookeeper server is restarted between test methods.</li>
 * <li>zookeeper client classes from previous tests methods get a "expired" exception upon reconnection to the new
 * zookeeper instance. With a fail-fast implementation, this would kill the current test.</li>
 * </ul>
 * 
 * 
 */
public class NonFailFastZookeeperClientsModule extends AbstractModule {

    public NonFailFastZookeeperClientsModule() {
    }

    @Override
    protected void configure() {
        bind(Assignment.class).to(AssignmentFromZKNoFailFast.class);
        bind(Cluster.class).to(ClusterFromZKNoFailFast.class);

        bind(Clusters.class).to(ClustersFromZKNoFailFast.class);

    }

}
