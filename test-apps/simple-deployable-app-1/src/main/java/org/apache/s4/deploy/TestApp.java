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

package org.apache.s4.deploy;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.zookeeper.CreateMode;

import com.google.common.collect.ImmutableList;

public class TestApp extends App {

    private ZkClient zkClient;

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        try {
            SimplePE prototype = createPE(SimplePE.class);
            Stream<Event> stream = createInputStream("inputStream", new KeyFinder<Event>() {
                public java.util.List<String> get(Event event) {
                    return ImmutableList.of("line");
                }
            }, prototype);
            zkClient = new ZkClient("localhost:" + 2181);
            if (!zkClient.exists("/s4-test")) {
                zkClient.create("/s4-test", null, CreateMode.PERSISTENT);
            }
            zkClient.createEphemeral(AppConstants.INITIALIZED_ZNODE_1, null);
        } catch (Exception e) {
            System.exit(-1);
        }
    }

    @Override
    protected void onStart() {
        try {
            Class.forName("org.apache.s4.deploy.A").getConstructor(ZkClient.class).newInstance(zkClient);
        } catch (Exception e) {
            System.exit(-1);
        }
    }
}
