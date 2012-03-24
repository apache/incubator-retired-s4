package org.apache.s4.deploy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.core.App;
import org.apache.s4.core.Server;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * 
 * <p>
 * Monitors applications on a given s4 cluster and starts them.
 * </p>
 * 
 * <p>
 * More specifically, this class observes the children of <code>/&lt;s4-cluster-name&gt;/apps</code>. Children
 * correspond to S4 applications. A child's metadata contains a URI that refers to the s4r file that contains the s4
 * application code.
 * </p>
 * 
 * <p>
 * At startup, existing applications are loaded by, for each detected application:
 * <ol>
 * <li>reading the associated URI
 * <li>fetching the s4r archive from that URI, through the protocol specified in the URI, and copying the s4r to a local
 * directory. Protocol handlers are not currently pluggable and must be implemented in this class.
 * <li>loading and starting the application
 * </ol>
 * 
 * <p>
 * Then, whenever new app children are detected, the deployment manager re-executes the above steps for those new
 * applications
 * </p>
 */
public class DistributedDeploymentManager implements DeploymentManager {

    public static final String S4R_URI = "s4r_uri";

    private static Logger logger = LoggerFactory.getLogger(DistributedDeploymentManager.class);

    private final String clusterName;

    final Set<String> apps = Sets.newHashSet();
    private final ZkClient zkClient;
    private final String appsPath;
    private final Server server;
    CountDownLatch signalInitialAppsLoaded = new CountDownLatch(1);

    @Inject
    public DistributedDeploymentManager(@Named("cluster.name") String clusterName,
            @Named("cluster.zk_address") String zookeeperAddress,
            @Named("cluster.zk_session_timeout") int sessionTimeout,
            @Named("cluster.zk_connection_timeout") int connectionTimeout, Server server) {

        this.clusterName = clusterName;
        this.server = server;

        zkClient = new ZkClient(zookeeperAddress, sessionTimeout, connectionTimeout);
        zkClient.setZkSerializer(new ZNRecordSerializer());
        IZkChildListener appListener = new AppsChangeListener();
        appsPath = "/" + clusterName + "/apps";
        if (!zkClient.exists(appsPath)) {
            zkClient.create(appsPath, null, CreateMode.PERSISTENT);
        }
        zkClient.subscribeChildChanges(appsPath, appListener);
    }

    public void deployApplication(String newApp) throws DeploymentFailedException {
        ZNRecord appData = zkClient.readData(appsPath + "/" + newApp);
        String uriString = appData.getSimpleField(S4R_URI);
        try {
            URI uri = new URI(uriString);

            // fetch application
            final File s4rFile = new File(server.getS4RDir() + File.separator + clusterName + File.separator + newApp
                    + ".s4r");
            if (s4rFile.exists()) {
                s4rFile.delete();
            }
            try {
                Files.createParentDirs(s4rFile);
                s4rFile.createNewFile();
            } catch (IOException e1) {
                throw new DeploymentFailedException("Cannot deploy application [" + newApp + "] from URI ["
                        + uri.toString() + "] ", e1);
            }
            try {
                if (ByteStreams.copy(fetchS4App(uri), Files.newOutputStreamSupplier(s4rFile)) == 0) {
                    throw new DeploymentFailedException("Cannot copy archive from [" + uri.toString() + "] to ["
                            + s4rFile.getAbsolutePath() + "] (nothing was copied)");
                }
            } catch (IOException e) {
                throw new DeploymentFailedException("Cannot deploy application [" + newApp + "] from URI ["
                        + uri.toString() + "] ", e);
            }
            // install locally
            App loaded = server.loadApp(s4rFile);
            if (loaded != null) {
                logger.info("Successfully installed application {}", newApp);
                server.startApp(loaded, newApp, clusterName);
            } else {
                throw new DeploymentFailedException("Cannot deploy application [" + newApp + "] from URI ["
                        + uri.toString() + "] : cannot start application");
            }
            // TODO sync with other nodes? (e.g. wait for other apps deployed before starting?

        } catch (URISyntaxException e) {
            logger.error("Cannot deploy app {} : invalid uri for fetching s4r archive {} : {} ", new String[] { newApp,
                    uriString, e.getMessage() });
            throw new DeploymentFailedException("Cannot deploy application [" + newApp + "]", e);
        }
    }

    // NOTE: in theory, we could support any protocol by implementing a chained visitor scheme,
    // but that's probably not that useful, and we can simply provide whichever protocol is needed
    public InputStream fetchS4App(URI uri) throws DeploymentFailedException {
        String scheme = uri.getScheme();
        if ("file".equalsIgnoreCase(scheme)) {
            return new FileSystemS4RFetcher().fetch(uri);
        }
        if ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) {
            return new HttpS4RFetcher().fetch(uri);
        }
        throw new DeploymentFailedException("Unsupported protocol " + scheme);
    }

    // synchronizes with startup apps loading
    private void deployNewApps(Set<String> newApps) {
        try {
            signalInitialAppsLoaded.await();
        } catch (InterruptedException e1) {
            logger.error("Interrupted while waiting for initialization of initial application. Cancelling deployment of new applications.");
            Thread.currentThread().interrupt();
            return;
        }
        deployApps(newApps);
    }

    private void deployApps(Set<String> newApps) {
        for (String newApp : newApps) {
            try {
                deployApplication(newApp);
                apps.add(newApp);
            } catch (DeploymentFailedException e) {
                logger.error("Application deployment failed for {}", newApp);
            }
        }
    }

    private final class AppsChangeListener implements IZkChildListener {
        @Override
        public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
            SetView<String> newApps = Sets.difference(Sets.newHashSet(currentChildren), apps);
            logger.info("Detected new application(s) to deploy {}" + Arrays.toString(newApps.toArray(new String[] {})));

            deployNewApps(newApps);

        }

    }

    @Override
    public void start() {
        List<String> initialApps = zkClient.getChildren(appsPath);
        deployApps(new HashSet<String>(initialApps));
        signalInitialAppsLoaded.countDown();
    }

}
