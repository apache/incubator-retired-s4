package org.apache.s4.comm.tcp;

import org.apache.s4.comm.util.PartitionInfo;
import org.junit.Assert;
import org.slf4j.LoggerFactory;

public class NetworkGlitchTest extends TCPBasedTest {
    public NetworkGlitchTest() {
        super(2);
        logger = LoggerFactory.getLogger(NetworkGlitchTest.class);
    }

    @Override
    public void testDelivery() throws InterruptedException {
        PartitionInfo util = partitions[0];

        startThreads();

        for (int i = 1; i < 10; i++) {
            Thread.sleep(100);
            ((TCPEmitter) util.emitter).closeAndRemoveChannel(0);
        }

        waitForThreads();

        Assert.assertTrue("Message delivery", messageDelivery());

        logger.info("Message ordering - " + messageOrdering());
        Assert.assertTrue("Pairwise message ordering", messageOrdering());
    }
}
