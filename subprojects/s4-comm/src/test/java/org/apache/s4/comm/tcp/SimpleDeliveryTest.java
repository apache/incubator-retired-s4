package org.apache.s4.comm.tcp;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleDeliveryTest extends TCPCommTest {

    private static Logger logger = LoggerFactory.getLogger(SimpleDeliveryTest.class);

    public SimpleDeliveryTest() throws IOException {
        super();
        logger = LoggerFactory.getLogger(SimpleDeliveryTest.class);
    }
}
