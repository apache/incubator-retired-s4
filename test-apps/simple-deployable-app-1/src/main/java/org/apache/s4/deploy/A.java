package org.apache.s4.deploy;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.deploy.AppConstants;

public class A {

    public A(ZkClient zkClient) {
        try {
            zkClient.createEphemeral(AppConstants.STARTED_ZNODE_1, null);
        } catch (Exception e) {
            System.exit(-1);
        }

    }

}
