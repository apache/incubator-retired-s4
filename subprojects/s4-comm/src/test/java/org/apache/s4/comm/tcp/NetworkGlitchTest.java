package org.apache.s4.comm.tcp;

import org.apache.s4.comm.util.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkGlitchTest extends TCPBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(NetworkGlitchTest.class);

    @Test
    public void testResilienceToNetworkGlitches() throws InterruptedException {
        PartitionInfo util = partitions[0];

        startThreads();

        for (int i = 1; i < 10; i++) {
            Thread.sleep(100);
            ((TCPEmitter) util.emitter).closeChannel(0);
        }

        waitForThreads();

        int messagesNotReceived = notDeliveredMessages();
        logger.info("# Messages not received = " + messagesNotReceived);
        Assert.assertEquals("Guaranteed message delivery", messagesNotReceived, 0);

        logger.info("Message ordering - " + messageOrdering());
        Assert.assertTrue("Pairwise message ordering", messageOrdering());
    }
}
