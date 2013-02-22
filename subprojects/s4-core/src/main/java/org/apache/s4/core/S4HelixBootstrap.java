package org.apache.s4.core;

import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.s4.comm.helix.S4HelixConstants;
import org.apache.s4.comm.helix.TaskStateModelFactory;
import org.apache.s4.comm.topology.AssignmentFromHelix;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.util.ArchiveFetchException;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.deploy.AppStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

/**
 * This is the bootstrap for S4 nodes.
 * <p>
 * Its roles are to:
 * <ul>
 * <li>register within the S4 cluster (and acquire a partition).
 * <li>look for application deployed on the S4 cluster
 * </ul>
 * <p>
 * When an application is available, custom modules are fetched if necessary and a full-featured S4 node is started. The
 * application code is then downloaded and the app started.
 * <p>
 * For testing purposes, it is also possible to directly start an application without fetching remote code, provided the
 * application classes are available in the classpath.
 * 
 * 
 * 
 */
public class S4HelixBootstrap implements Bootstrap {
    private static Logger logger = LoggerFactory.getLogger(S4HelixBootstrap.class);

    private final AtomicBoolean deployed = new AtomicBoolean(false);

    private final ArchiveFetcher fetcher;

    // private Injector parentInjector;

    CountDownLatch signalOneAppLoaded = new CountDownLatch(1);

    private final String clusterName;

    private String instanceName;

    private final String zookeeperAddress;

    private final AppStateModelFactory appStateModelFactory;
    private final AssignmentFromHelix assignmentFromHelix;

    private final Cluster cluster;

    public static Injector rootInjector;

    @Inject
    public S4HelixBootstrap(@Named("s4.cluster.name") String clusterName,
            @Named("s4.cluster.zk_address") String zookeeperAddress,
            @Named("s4.cluster.zk_session_timeout") int sessionTimeout,
            @Named("s4.cluster.zk_connection_timeout") int connectionTimeout,
            AppStateModelFactory appStateModelFactory, AssignmentFromHelix assignmentFromHelix,
            ArchiveFetcher fetcher, Cluster cluster) {
        this.clusterName = clusterName;
        this.zookeeperAddress = zookeeperAddress;
        this.appStateModelFactory = appStateModelFactory;
        this.fetcher = fetcher;
        this.cluster = cluster;
        this.assignmentFromHelix = assignmentFromHelix;
    }

    @Override
    public void start(Injector parentInjector) throws InterruptedException, ArchiveFetchException, UnknownHostException {

        // start a HelixController to manage the cluster
        // TODO set this as optional (small clusters only)
        String controllerName = Inet4Address.getLocalHost().getCanonicalHostName() + UUID.randomUUID().toString();
        HelixControllerMain.startHelixController(zookeeperAddress, S4HelixConstants.HELIX_CLUSTER_NAME, controllerName,
                HelixControllerMain.STANDALONE);
        // this.parentInjector = parentInjector;
        S4HelixBootstrap.rootInjector = parentInjector;
        
        registerWithHelix();

        signalOneAppLoaded.await();
    }

    private void registerWithHelix() {
        HelixManager helixManager = assignmentFromHelix.getHelixManager();
        try {
            helixManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline", appStateModelFactory);
            helixManager.addExternalViewChangeListener((RoutingTableProvider) cluster);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
