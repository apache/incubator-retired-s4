package org.apache.s4.comm.udp;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDeliveryTest extends UDPBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleDeliveryTest.class);

    public SimpleDeliveryTest() {
        super();
    }

    /**
     * Tests the protocol. If all components function without throwing exceptions, the test passes.
     * 
     * @throws InterruptedException
     */
    @Test
    public void testUDPDelivery() {
        try {
            startThreads();
            waitForThreads();
            logger.info("# Messages not received = " + notDeliveredMessages());
        } catch (Exception e) {
            Assert.fail("UDP Simple DeliveryTest");
        }
    }
}
