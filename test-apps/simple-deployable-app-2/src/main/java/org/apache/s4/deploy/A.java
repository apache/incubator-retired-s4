package org.apache.s4.deploy;

import org.I0Itec.zkclient.ZkClient;

public class A {

    public A(ZkClient zkClient) {
        try {
            zkClient.createEphemeral(AppConstants.STARTED_ZNODE_2, null);
        } catch (Exception e) {
            System.exit(-1);
        }

    }

}
