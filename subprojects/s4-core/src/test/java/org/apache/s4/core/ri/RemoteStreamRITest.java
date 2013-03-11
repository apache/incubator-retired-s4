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

package org.apache.s4.core.ri;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.s4.comm.tools.TaskSetup;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.util.AppConfig;
import org.apache.s4.deploy.DeploymentUtils;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.s4.wordcount.WordCountModule;

import com.google.common.collect.ImmutableList;

/**
 * Resue {@link RuntimeIsolationTest} but using an external adapter instead of injecting the events directly.
 * 
 * Test case is {@link RuntimeIsolationTest.testSimple}
 * 
 */
public class RemoteStreamRITest extends RuntimeIsolationTest {

    @Override
    public void startNodes() throws IOException, InterruptedException {
        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.setup("cluster2", 1, 1500);

        s4nodes = new Process[numberTasks + 1];

        List<Process> nodes = new ArrayList<Process>();

        DeploymentUtils.initAppConfig(new AppConfig.Builder().appClassName(IsolationWordCountApp.class.getName())
                .customModulesNames(ImmutableList.of(WordCountModule.class.getName())).build(), "cluster1", false,
                "localhost:2181");
        nodes.addAll(Arrays.asList(CoreTestUtils.forkS4Nodes(new String[] { "-c", "cluster1" }, new ZkClient(
                "localhost:2181"), 10, "cluster1", numberTasks)));

        DeploymentUtils.initAppConfig(new AppConfig.Builder().appClassName(RemoteAdapterApp.class.getName())
                .customModulesNames(ImmutableList.of(WordCountModule.class.getName())).build(), "cluster2", false,
                "localhost:2181");

        nodes.addAll(Arrays.asList(CoreTestUtils.forkS4Nodes(new String[] { "-c", "cluster2" }, new ZkClient(
                "localhost:2181"), 10, "cluster2", 1)));

        s4nodes = nodes.toArray(new Process[] {});
    }
    
    @Override
    public void createEmitter() throws IOException {
        // No need for an emitter, we use an adapter
    }

    @Override
    public void injectData() throws InterruptedException, IOException {
        // No nedd for data injection, we use an adapter
    }
}
