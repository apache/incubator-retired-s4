package org.apache.s4.comm.topology;

import java.io.File;
import java.io.FileOutputStream;
import java.net.InetAddress;
import java.nio.channels.FileLock;

import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Implements the assignment interface {@link Assignment} using a file lock.
 * 
 */
public class AssignmentFromFile implements Assignment {
    private static final Logger logger = LoggerFactory.getLogger(AssignmentFromFile.class);
    final private Cluster cluster;
    final private String lockDir;

    @Inject
    public AssignmentFromFile(Cluster cluster, @Named("cluster.lock_dir") String lockDir) {
        this.cluster = cluster;
        this.lockDir = lockDir;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.s4.comm.topology.Assignment#assignClusterNode()
     */
    @Override
    public ClusterNode assignClusterNode() {
        while (true) {
            try {
                for (ClusterNode node : cluster.getNodes()) {
                    boolean partitionAvailable = canTakeupProcess(node);
                    logger.info("Partition available: " + partitionAvailable);
                    if (partitionAvailable) {
                        boolean success = takeProcess(node);
                        logger.info("Acquire partition:" + ((success) ? "success." : "failure."));
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
                    String message = "Partition acquired by PID:" + getPID() + " HOST:"
                            + InetAddress.getLocalHost().getHostName();
                    fos.write(message.getBytes());
                    fos.close();
                    logger.info(message + "  Lock File location: " + lockFile.getAbsolutePath());
                    return true;
                }
            }
        } catch (Exception e) {
            logger.error("Exception trying to take up partition:" + node.getPartition(), e);
        } finally {
            if (lockFile != null) {
                lockFile.deleteOnExit();
            }
        }
        return false;
    }

    private String createLockFileName(ClusterNode node) {
        String lockFileName = "s4-" + node.getPartition() + ".lock";
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
            logger.info("Host Name: " + InetAddress.getLocalHost().getCanonicalHostName());
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
            logger.info("Partition taken up by another process lockFile:" + lockFileName);
        }
        return false;
    }

    private long getPID() {
        String processName = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(processName.split("@")[0]);
    }
}
