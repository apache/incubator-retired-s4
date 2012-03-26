package org.apache.s4.tools;

import java.io.File;
import java.util.Arrays;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

public class ZKServer {

    static Logger logger = LoggerFactory.getLogger(ZKServer.class);

    public static void main(String[] args) {
        ZKServerArgs zkArgs = new ZKServerArgs();
        JCommander jc = new JCommander(zkArgs);
        try {
            jc.parse(args);
        } catch (Exception e) {
            System.out.println(Arrays.toString(args));
            e.printStackTrace();
            jc.usage();
            System.exit(-1);
        }
        try {

            logger.info("Starting zookeeper server on port [{}]", zkArgs.zkPort);

            if (zkArgs.clean) {
                logger.info("cleaning existing data in [{}] and [{}]", zkArgs.dataDir, zkArgs.logDir);
                FileUtils.deleteDirectory(new File(zkArgs.dataDir));
                FileUtils.deleteDirectory(new File(zkArgs.logDir));
            }
            IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {

                @Override
                public void createDefaultNameSpace(ZkClient zkClient) {

                }
            };

            ZkServer zkServer = new ZkServer(zkArgs.dataDir, zkArgs.logDir, defaultNameSpace);
            zkServer.start();
        } catch (Exception e) {
            logger.error("Cannot initialize zookeeper with specified configuration", e);
        }
    }

    @Parameters(separators = "=", commandDescription = "Start Zookeeper server")
    static class ZKServerArgs {

        @Parameter(names = "-port", description = "Zookeeper port")
        String zkPort = "2181";

        @Parameter(names = "-dataDir", description = "data directory", required = false)
        String dataDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "tmp" + File.separator
                + "zookeeper" + File.separator + "data").getAbsolutePath();

        @Parameter(names = "-clean", description = "clean zookeeper data (arity=0) (make sure you specify correct directories...)")
        boolean clean = true;

        @Parameter(names = "-logDir", description = "log directory")
        String logDir = new File(System.getProperty("java.io.tmpdir") + File.separator + "tmp" + File.separator
                + "zookeeper" + File.separator + "log").getAbsolutePath();

    }

}
