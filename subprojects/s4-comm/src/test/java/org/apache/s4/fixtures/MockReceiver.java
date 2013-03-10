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

import java.nio.ByteBuffer;

import org.apache.s4.base.Receiver;
import org.apache.s4.base.SerializerDeserializer;

import com.google.inject.Inject;

/**
 * For tests purposes, intercepts messages that would normally be delegated to the application layer.
 * 
 */
public class MockReceiver implements Receiver {

    SerializerDeserializer serDeser;

    @Inject
    public MockReceiver(SerializerDeserializer serDeser) {
        super();
        this.serDeser = serDeser;
    }

    @Override
    public void receive(ByteBuffer message) {
        if (CommTestUtils.MESSAGE.equals(serDeser.deserialize(message))) {
            CommTestUtils.SIGNAL_MESSAGE_RECEIVED.countDown();
        } else {
            System.err.println("Unexpected message:" + serDeser.deserialize(message));
        }

    }

    @Override
    public int getPartitionId() {
        throw new RuntimeException("Not implemented");
    }
}
