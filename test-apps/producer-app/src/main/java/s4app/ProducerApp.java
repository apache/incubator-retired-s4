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

import org.apache.s4.comm.topology.ZNRecord;
import org.apache.s4.comm.topology.ZNRecordSerializer;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.App;
import org.apache.zookeeper.CreateMode;

public class ProducerApp extends App {

    private ProducerPE producerPE;

    @Override
    protected void onStart() {
        System.out.println("Starting CounterApp...");
        ((ProducerPE) producerPE.getInstanceForKey("single")).sendMessages();
    }

    // generic array due to varargs generates a warning.
    @Override
    protected void onInit() {
        System.out.println("Initing CounterApp...");

        ZkClient zkClient = new ZkClient("localhost:2181");

        zkClient.setZkSerializer(new ZNRecordSerializer());
        ZNRecord record = new ZNRecord(Thread.currentThread().getContextClassLoader().getClass().getName());
        zkClient.create("/s4/classLoader", record, CreateMode.PERSISTENT);

        producerPE = createPE(ProducerPE.class, "producer");
        producerPE.setStreams(createOutputStream("tickStream"));

    }

    @Override
    protected void onClose() {
    }

}
