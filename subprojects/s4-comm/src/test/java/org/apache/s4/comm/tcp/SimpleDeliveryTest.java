package org.apache.s4.comm.tcp;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDeliveryTest extends TCPBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleDeliveryTest.class);

    @Test
    public void testSimpleDelivery() throws InterruptedException {
        startThreads();
        waitForThreads();

        int messagesNotReceived = notDeliveredMessages();
        logger.info("# Messages not received = " + messagesNotReceived);
        Assert.assertEquals("Guaranteed message delivery", messagesNotReceived, 0);

        logger.info("Message ordering - " + messageOrdering());
        Assert.assertTrue("Pairwise message ordering", messageOrdering());
    }
}
