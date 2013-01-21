package org.apache.s4.core.moduleloader;

import java.io.IOException;

import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.After;
import org.junit.Test;

// uses a separate class from TestModuleLoader in order to use a clean environment (tests are assumed to forked)
public class TestModuleLoaderRemote extends ZkBasedTest {

    private Process forkedS4Node;

    @Test
    public void testRemote() throws Exception {
        forkedS4Node = ModuleLoaderTestUtils.testModuleLoader(true);
    }

    @After
    public void cleanUp() throws IOException, InterruptedException {
        CoreTestUtils.killS4App(forkedS4Node);
    }

}
