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

package org.apache.s4.core.ft;

import java.io.IOException;

import org.apache.s4.core.ProcessingElement;
import org.apache.s4.fixtures.CoreTestUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class StatefulTestPE extends ProcessingElement {

    String id;
    String value1 = "";
    String value2 = "";
    transient ZooKeeper zk = null;

    public void onEvent(org.apache.s4.base.Event event) {
        try {

            if ("setValue1".equals(event.get("command"))) {
                setValue1(event.get("value"));
                zk.create("/value1Set", event.get("value").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else if ("setValue2".equals(event.get("command"))) {
                setValue2(event.get("value"));
                zk.create("/value2Set", event.get("value").getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else if ("checkpoint".equals(event.get("command"))) {
                checkpoint();
            } else {
                throw new RuntimeException("unidentified event: " + event);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(String value1) {
        this.value1 = value1;
        persistValues();
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
        persistValues();
    }

    public void setId(String id) {
        this.id = id;
    }

    // NOTE: we use a file as a simple way to keep track of changes
    private void persistValues() {

        try {
            try {
                zk.delete("/data", -1);
            } catch (NoNodeException ignored) {
            }
            zk.create("/data", ("value1=" + value1 + " ; value2=" + value2).getBytes(), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (KeeperException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected void onCreate() {
        try {
            zk = CoreTestUtils.createZkClient();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
