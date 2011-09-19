package io.s4.comm.topology;


import io.s4.comm.topology.Cluster;
import io.s4.comm.topology.ClusterNode;
import io.s4.core.Stream;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.nio.channels.FileLock;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignmentFromFile implements Assignment {
    private static final Logger logger = LoggerFactory.getLogger(Stream.class);
    Set<Map<String, String>> processSet = new HashSet<Map<String, String>>();
    private String clusterName;
    private String clusterConfigurationFilename;
    private Cluster cluster;

    public AssignmentFromFile(String clusterName,
            String clusterConfigurationFilename) {
        this.clusterName = clusterName;
        this.clusterConfigurationFilename = clusterConfigurationFilename;
        // read the configuration file
        readStaticConfig();
    }
    
    public ClusterNode assignPartition() {
        while (true) {
            try {
                for (ClusterNode node : cluster.getNodes()) {
                    boolean partitionAvailable = canTakeupProcess(node);
                    logger.info("partition available: " + partitionAvailable);
                    if (partitionAvailable) {
                        boolean success = takeProcess(node);
                        logger.info("Acquire partition:"
                                + ((success) ? "Success" : "failure"));
                        if (success) {
                            return node;
                        }
                    }
                }
                Thread.sleep(5000);

            } catch (Exception e) {
                logger.error("Exception in assignPartition Method:", e);
            }

        }

    }

    private void readStaticConfig() {
        ConfigParser parser = new ConfigParser();
        Config config = parser.parse(clusterConfigurationFilename);

        // find the requested cluster
        for (Cluster checkCluster : config.getClusters()) {
            if (checkCluster.getName().equals(clusterName)) {
                cluster = checkCluster;
                break;
            }
        }
        if (cluster == null) {
            throw new RuntimeException("Cluster " + clusterName + " not configured");
        }
    }

    private boolean takeProcess(ClusterNode node) {
        File lockFile = null;
        try {
            // TODO:contruct from processConfig
            String lockFileName = createLockFileName(node);
            lockFile = new File(lockFileName);
            if (!lockFile.exists()) {
                FileOutputStream fos = new FileOutputStream(lockFile);
                FileLock fl = fos.getChannel().tryLock();
                if (fl != null) {
                    String message = "Partition acquired by PID:"
                            + getPID() + " HOST:"
                            + InetAddress.getLocalHost().getHostName();
                    fos.write(message.getBytes());
                    fos.close();
                    logger.info(message + "  Lock File location: "
                            + lockFile.getAbsolutePath());
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("Exception trying to take up partition:" + node.getPartition(),
                         e);
        } finally {
            if (lockFile != null) {
                lockFile.deleteOnExit();
            }
        }
        return false;
    }

    private String createLockFileName(ClusterNode node) {
        String lockDir = System.getProperty("lock_dir");
        String lockFileName = clusterName + node.getPartition();
        if (lockDir != null && lockDir.trim().length() > 0) {
            File file = new File(lockDir);
            if (!file.exists()) {
                file.mkdirs();
            }
            return lockDir + "/" + lockFileName;
        } else {
            return lockFileName;
        }
    }

    private boolean canTakeupProcess(ClusterNode node) {
        try {
            InetAddress inetAddress = InetAddress.getByName(node.getMachineName());
            logger.info("Host Name: "
                    + InetAddress.getLocalHost().getCanonicalHostName());
            if (!node.getMachineName().equals("localhost")) {
                if (!InetAddress.getLocalHost().equals(inetAddress)) {
                    return false;
                }
            }
        } catch (Exception e) {
            logger.error("Invalid host:" + node.getMachineName());
            return false;
        }
        String lockFileName = createLockFileName(node);
        File lockFile = new File(lockFileName);
        if (!lockFile.exists()) {
            return true;
        } else {
            logger.info("Partition taken up by another process lockFile:"
                    + lockFileName);
        }
        return false;
    }
    
    public static long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean()
                                                                   .getName();
        return Long.parseLong(processName.split("@")[0]);
    }
}
