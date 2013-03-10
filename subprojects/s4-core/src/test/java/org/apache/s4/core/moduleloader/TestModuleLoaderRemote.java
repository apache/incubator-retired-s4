/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
