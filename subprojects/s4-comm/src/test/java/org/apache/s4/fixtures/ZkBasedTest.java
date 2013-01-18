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

package org.apache.s4.fixtures;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;

import junit.framework.Assert;

import org.apache.s4.comm.tools.TaskSetup;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.server.NIOServerCnxn.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ZkBasedTest {
    private static final Logger logger = LoggerFactory.getLogger(ZkBasedTest.class);

    private Factory zkFactory;
    protected final int numTasks;

    protected ZkBasedTest() {
        this.numTasks = 1;
    }

    protected ZkBasedTest(int numTasks) {
        this.numTasks = numTasks;
    }

    @BeforeClass
    public static void initClass() {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            @Override
            public void uncaughtException(Thread t, Throwable e) {
                logger.error("Uncaught error in thread {}: {}", t.getName(), e);
                Assert.fail("Uncaught error in thread " + t.getName() + " : " + e.getMessage());
            }
        });
    }

    @Before
    public void prepare() throws IOException, InterruptedException, KeeperException {

        CommTestUtils.cleanupTmpDirs();

        zkFactory = CommTestUtils.startZookeeperServer();

        TaskSetup taskSetup = new TaskSetup("localhost:" + CommTestUtils.ZK_PORT);
        taskSetup.clean("s4");
        taskSetup.setup("cluster1", numTasks, 1300);
    }

    @After
    public void cleanupZkBasedTest() throws IOException, InterruptedException {
        CommTestUtils.stopZookeeperServer(zkFactory);
        CommTestUtils.cleanupTmpDirs();
    }
}
