package org.apache.s4.comm.tcp;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiPartitionDeliveryTest extends TCPBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(MultiPartitionDeliveryTest.class);

    public MultiPartitionDeliveryTest() {
        super(4);
    }

    @Test
    public void testMultiPartitionDelivery() throws InterruptedException {

        startThreads();
        waitForThreads();

        int messagesNotReceived = notDeliveredMessages();
        logger.info("# Messages not received = " + messagesNotReceived);
        Assert.assertEquals("Guaranteed message delivery", messagesNotReceived, 0);

        logger.info("Message ordering - " + messageOrdering());
        Assert.assertTrue("Pairwise message ordering", messageOrdering());
    }
}
