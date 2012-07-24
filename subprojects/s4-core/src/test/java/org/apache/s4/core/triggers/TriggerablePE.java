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

package org.apache.s4.core.triggers;

import java.io.IOException;

import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.fixtures.CommTestUtils;
import org.apache.s4.wordcount.StringEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class TriggerablePE extends ProcessingElement implements Watcher {

    private ZooKeeper zk;

    public TriggerablePE() {
    }

    public TriggerablePE(App app) {
        super(app);
    }

    public void onEvent(StringEvent event) {
        try {
            zk.create("/onEvent@" + event.getString(), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onCreate() {
        if (zk == null) {
            try {
                zk = new ZooKeeper("localhost:" + CommTestUtils.ZK_PORT, 4000, this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void onTrigger(StringEvent event) {
        try {
            zk.create("/onTrigger[StringEvent]@" + event.getString(), new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void onRemove() {
        // TODO Auto-generated method stub

    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub

    }

}
