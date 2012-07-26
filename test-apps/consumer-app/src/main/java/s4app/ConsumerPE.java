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

package s4app;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerPE.class);
    long eventCount = 0;

    public ConsumerPE(App app) {
        super(app);
    }

    public void onEvent(Event event) {
        eventCount++;
        logger.trace(
                "Received event with tick {} and time {} for event # {}",
                new String[] { String.valueOf(event.get("tick", Long.class)), String.valueOf(event.getTime()),
                        String.valueOf(eventCount) });
        if (eventCount == 100000) {
            logger.info("Just reached 100000 events");
            ZkClient zkClient = new ZkClient("localhost:2181");
            zkClient.create("/AllTicksReceived", new byte[0], CreateMode.PERSISTENT);
        }

    }

    @Override
    protected void onRemove() {

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }
}
