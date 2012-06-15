package org.apache.s4.comm.tcp;

import java.io.IOException;

import org.apache.s4.comm.util.PartitionInfo;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NetworkGlitchTest extends TCPCommTest {

    private static Logger logger = LoggerFactory.getLogger(NetworkGlitchTest.class);

    public NetworkGlitchTest() throws IOException {
        super(2);
        logger = LoggerFactory.getLogger(NetworkGlitchTest.class);
    }

    public void testDelivery() throws InterruptedException {
        PartitionInfo util = partitions[0];

        startThreads();

        for (int i = 0; i < 4; i++) {
            Thread.sleep(500);
            logger.debug("Messages sent so far - {}", util.sendThread.sendCounts);
            ((TCPEmitter) util.emitter).removeChannel(0);
            logger.debug("Channel closed");
        }

        waitForThreads();

        Assert.assertTrue("Message delivery", messageDelivery());

        logger.info("Message ordering - " + messageOrdering());
        Assert.assertTrue("Pairwise message ordering", messageOrdering());
    }
}
