package org.apache.s4.core.moduleloader;

import org.junit.Test;

// separated from its parent in order to start with clean environment with forked tests
public class TestModuleLoaderRemote extends TestModuleLoader {

    @Test
    public void testRemote() throws Exception {
        testModuleLoader(true);
    }
}
