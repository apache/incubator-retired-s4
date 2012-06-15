package org.apache.s4.comm.tcp;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiPartitionDeliveryTest extends TCPCommTest {

    private static Logger logger = LoggerFactory.getLogger(MultiPartitionDeliveryTest.class);

    public MultiPartitionDeliveryTest() throws IOException {
        super(6);
        logger = LoggerFactory.getLogger(MultiPartitionDeliveryTest.class);
    }
}
