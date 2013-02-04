package org.apache.s4.core;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.s4.base.util.ModulesLoader;
import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.comm.ModulesLoaderFactory;
import org.apache.s4.comm.helix.TaskStateModelFactory;
import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.comm.util.ArchiveFetchException;
import org.apache.s4.comm.util.ArchiveFetcher;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.core.util.ParametersInjectionModule;
import org.apache.s4.deploy.AppStateModelFactory;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Named;
import com.google.inject.util.Modules;
import com.google.inject.util.Modules.OverriddenModuleBuilder;

/**
 * This is the bootstrap for S4 nodes.
 * <p>
 * Its roles are to:
 * <ul>
 * <li>register within the S4 cluster (and acquire a partition).
 * <li>look for application deployed on the S4 cluster
 * </ul>
 * <p>
 * When an application is available, custom modules are fetched if necessary and
 * a full-featured S4 node is started. The application code is then downloaded
 * and the app started.
 * <p>
 * For testing purposes, it is also possible to directly start an application
 * without fetching remote code, provided the application classes are available
 * in the classpath.
 * 
 * 
 * 
 */
public class S4HelixBootstrap implements Bootstrap{
	private static Logger logger = LoggerFactory
			.getLogger(S4HelixBootstrap.class);

	private final AtomicBoolean deployed = new AtomicBoolean(false);

	private final ArchiveFetcher fetcher;

	private Injector parentInjector;

	CountDownLatch signalOneAppLoaded = new CountDownLatch(1);

	private String clusterName;

	private String instanceName;

	private String zookeeperAddress;
    @Inject
    private TaskStateModelFactory taskStateModelFactory;
    
    @Inject
    private AppStateModelFactory appStateModelFactory;
    
    @Inject
    private Cluster cluster;

	@Inject
	public S4HelixBootstrap(@Named("s4.cluster.name") String clusterName,
            @Named("s4.instance.name") String instanceName,
			@Named("s4.cluster.zk_address") String zookeeperAddress,
			@Named("s4.cluster.zk_session_timeout") int sessionTimeout,
			@Named("s4.cluster.zk_connection_timeout") int connectionTimeout,
			ArchiveFetcher fetcher) {
        this.clusterName = clusterName;
        this.instanceName = instanceName;
        this.zookeeperAddress = zookeeperAddress;
		this.fetcher = fetcher;
        registerWithHelix();
	}

	public void start(Injector parentInjector) throws InterruptedException,
			ArchiveFetchException {
		this.parentInjector = parentInjector;
		if (!deployed.get()) {

		}
		signalOneAppLoaded.await();
	}
	
	private void registerWithHelix()
    {
      HelixManager helixManager;
      try
      {
        helixManager = HelixManagerFactory.getZKHelixManager(clusterName,
            instanceName, InstanceType.PARTICIPANT, zookeeperAddress);
        helixManager.getStateMachineEngine().registerStateModelFactory(
          "LeaderStandby", taskStateModelFactory);
        helixManager.getStateMachineEngine().registerStateModelFactory(
          "OnlineOffline", appStateModelFactory);
        helixManager.connect();  
        helixManager.addExternalViewChangeListener((RoutingTableProvider)cluster);
      } catch (Exception e)
      {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

}
