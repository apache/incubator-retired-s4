package org.apache.s4.comm.tcp;

import org.slf4j.LoggerFactory;

public class MultiPartitionDeliveryTest extends TCPBasedTest {
    public MultiPartitionDeliveryTest() {
        super(6);
        logger = LoggerFactory.getLogger(MultiPartitionDeliveryTest.class);
    }
}
