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

package org.apache.s4.comm.loopback;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.SerializerDeserializer;

import com.google.inject.Inject;

public class LoopBackEmitter implements Emitter {
    private LoopBackListener listener;

    @Inject
    SerializerDeserializer serDeser;

    public LoopBackEmitter(LoopBackListener listener) {
        this.listener = listener;
    }

    @Override
    public boolean send(int partitionId, EventMessage message) {

        listener.put(serDeser.serialize(message));
        return true;
    }

    @Override
    public int getPartitionCount() {
        return 1;
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
