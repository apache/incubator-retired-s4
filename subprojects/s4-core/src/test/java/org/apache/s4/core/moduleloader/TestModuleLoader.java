package org.apache.s4.core.moduleloader;

import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

public class TestModuleLoader extends ZkBasedTest {

    public TestModuleLoader() {
        // need 2 partitions: 1 for the test emitter, 1 for the S4 node
        super(2);
    }

    @Test
    public void testLocal() throws Exception {
        ModuleLoaderTestUtils.testModuleLoader(false);
    }

}
