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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

import org.apache.s4.base.Listener;

public class LoopBackListener implements Listener {

    private BlockingQueue<byte[]> handoffQueue = new SynchronousQueue<byte[]>();

    @Override
    public byte[] recv() {
        try {
            // System.out.println("LoopBackListener: Taking message from handoff queue");
            return handoffQueue.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getPartitionId() {
        return 0;
    }

    public void put(byte[] message) {
        try {
            // System.out.println("LoopBackListener: putting message into handoffqueue");
            handoffQueue.put(message);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
