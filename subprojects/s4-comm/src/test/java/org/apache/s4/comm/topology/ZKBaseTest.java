package org.apache.s4.comm.topology;

import java.io.File;

import junit.framework.TestCase;

import org.I0Itec.zkclient.IDefaultNameSpace;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkServer;

public class ZKBaseTest extends TestCase {
    protected ZkServer zkServer = null;
    protected ZkClient zkClient;
    protected String zookeeperAddress;

    @Override
    public void setUp() {
        String dataDir = System.getProperty("user.dir") + File.separator
                + "tmp" + File.separator + "zookeeper" + File.separator
                + "data";
        String logDir = System.getProperty("user.dir") + File.separator + "tmp"
                + File.separator + "zookeeper" + File.separator + "logs";
        IDefaultNameSpace defaultNameSpace = new IDefaultNameSpace() {

            @Override
            public void createDefaultNameSpace(ZkClient zkClient) {

            }
        };
        int port = 3029;
        zkServer = new ZkServer(dataDir, logDir, defaultNameSpace, port);
        zkServer.start();
        zkClient = zkServer.getZkClient();
        zookeeperAddress = "localhost:" + port;

    }
    public void test(){
        
    }
    @Override
    protected void tearDown() throws Exception {
        if (zkServer != null) {
            zkServer.shutdown();
        }
    }
}
