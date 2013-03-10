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
package org.apache.s4.benchmark.utils;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static Logger logger = LoggerFactory.getLogger(Utils.class);

    public static CountDownLatch getReadySignal(String zkString, final String parentPath, final int counts) {
        ZkClient zkClient = new ZkClient(zkString);
        if (zkClient.exists(parentPath)) {
            System.out.println(parentPath + " path exists and will be deleted");
            zkClient.deleteRecursive(parentPath);
        }
        zkClient.createPersistent(parentPath);
        final CountDownLatch signalReady = new CountDownLatch(1);
        zkClient.subscribeChildChanges(parentPath, new IZkChildListener() {

            @Override
            public void handleChildChange(String arg0, List<String> arg1) throws Exception {

                if (parentPath.equals(arg0)) {
                    if (arg1.size() >= counts) {
                        logger.info("Latch reached for {} with {} children", arg0, counts);
                        signalReady.countDown();
                    }
                }
            }
        });
        return signalReady;
    }

    public static void watchAndSignalChildrenReachedCount(final String path, final CountDownLatch latch,
            final ZooKeeper zk, final int count) throws KeeperException, InterruptedException {

        List<String> children = zk.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (EventType.NodeChildrenChanged.equals(event.getType())) {
                    try {
                        if (count == zk.getChildren(path, false).size()) {
                            latch.countDown();
                        }
                    } catch (KeeperException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            }
        });
        if (children.size() == count) {
            latch.countDown();
        }
    }
}
