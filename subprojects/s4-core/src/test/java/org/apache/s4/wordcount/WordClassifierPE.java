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

package org.apache.s4.wordcount;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordClassifierPE extends ProcessingElement implements Watcher {

    TreeMap<String, Integer> counts = new TreeMap<String, Integer>();
    private int counter;
    transient private ZooKeeper zk;

    private static Logger logger = LoggerFactory.getLogger(WordClassifierPE.class);

    private WordClassifierPE() {
    }

    public WordClassifierPE(App app) {
        super(app);
    }

    public void onEvent(WordCountEvent event) {
        try {
            WordCountEvent wcEvent = event;
            if (zk == null) {
                try {
                    zk = new ZooKeeper("localhost:2181", 4000, this);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            logger.info("seen: " + wcEvent.getWord() + "/" + wcEvent.getCount());

            if (!counts.containsKey(wcEvent.getWord())
                    || (counts.containsKey(wcEvent.getWord()) && counts.get(wcEvent.getWord()).compareTo(
                            wcEvent.getCount()) < 0)) {
                // this is because wcEvent events arrive unordered
                counts.put(wcEvent.getWord(), wcEvent.getCount());
            }
            ++counter;
            if (counter == WordCountTest.TOTAL_WORDS) {
                File results = new File(CommTestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "wordcount");
                if (results.exists()) {
                    if (!results.delete()) {
                        throw new RuntimeException("cannot delete results file");
                    }
                }
                Set<Entry<String, Integer>> entrySet = counts.entrySet();
                StringBuilder sb = new StringBuilder();
                for (Entry<String, Integer> entry : entrySet) {
                    sb.append(entry.getKey() + "=" + entry.getValue() + ";");
                }

                try {
                    zk.delete("/results", -1);
                } catch (NoNodeException ignored) {
                }

                zk.create("/results", sb.toString().getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            } else {
                // NOTE: this will fail if we did not recover the latest
                // counter,
                // because there is already a counter with this number in
                // zookeeper
                zk.create("/classifierIteration_" + counter, new byte[counter], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                logger.debug("wrote classifier iteration [" + counter + "]");
                System.out.println("wrote classifier iteration [" + counter + "]");
                // check if we are allowed to continue
                if (null == zk.exists("/continue_" + counter, null)) {
                    CountDownLatch latch = new CountDownLatch(1);
                    CommTestUtils.watchAndSignalCreation("/continue_" + counter, latch, zk);
                    latch.await();
                } else {
                    zk.delete("/continue_" + counter, -1);
                    System.out.println("");
                }
            }

        } catch (Exception e) {
            // TODO should propagate some exceptions
            e.printStackTrace();
        }

    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
