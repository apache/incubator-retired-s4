package org.apache.s4.deploy;

import java.io.IOException;

import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

public class SimplePE extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(SimplePE.class);
    private ZkClient zk;

    public SimplePE() {
    }

    public SimplePE(App app) {
        super(app);
    }

    public void onEvent(org.apache.s4.base.Event event) {
        try {
            LoggerFactory.getLogger(getClass()).debug("processing envent {}", event.get("line"));
            // test s4r resource access
            zk.create("/resourceData",
                    new String(ByteStreams.toByteArray(getClass().getResourceAsStream("/resource.txt"))),
                    CreateMode.PERSISTENT);
            // test event processing
            zk.create("/onEvent@" + event.get("line"), new byte[0], CreateMode.PERSISTENT);
            zk.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected void onCreate() {
        if (zk == null) {
            zk = new ZkClient("localhost:" + 2181);
        }

    }

    @Override
    protected void onRemove() {
    }

}
