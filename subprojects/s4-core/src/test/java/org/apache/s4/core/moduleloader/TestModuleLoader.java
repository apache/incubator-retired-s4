package org.apache.s4.core.moduleloader;

import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

public class TestModuleLoader extends ZkBasedTest {

    @Test
    public void testLocal() throws Exception {
        ModuleLoaderTestUtils.testModuleLoader(false);
    }

}
