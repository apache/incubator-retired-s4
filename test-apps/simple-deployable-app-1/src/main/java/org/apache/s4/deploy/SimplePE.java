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

import java.io.IOException;

import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

public class SimplePE extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(SimplePE.class);
    private ZkClient zk;

    public SimplePE() {
    }

    public SimplePE(App app) {
        super(app);
    }

    public void onEvent(org.apache.s4.base.Event event) {
        try {
            LoggerFactory.getLogger(getClass()).debug("processing envent {}", event.get("line"));
            // test s4r resource access
            // need to strip ASL text from reference content file
            String strippedContent = new String(
                    ByteStreams.toByteArray(getClass().getResourceAsStream("/resource.txt"))).substring(new String(
                    ByteStreams.toByteArray(getClass().getResourceAsStream("/ASL2.txt"))).length());

            zk.create("/resourceData", strippedContent, CreateMode.PERSISTENT);
            // test event processing
            zk.create("/onEvent@" + event.get("line"), new byte[0], CreateMode.PERSISTENT);
            zk.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    protected void onCreate() {
        if (zk == null) {
            zk = new ZkClient("localhost:" + 2181);
        }

    }

    @Override
    protected void onRemove() {
    }

}
