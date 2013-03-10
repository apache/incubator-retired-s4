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
package org.apache.s4.benchmark.prodcon;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.App;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;

public class ProducerConsumerApp extends App {

    @Inject
    @Named("s4.cluster.zk_address")
    String zkString;

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub
    }

    @Override
    protected void onInit() {
        File logDirectory = new File(System.getProperty("user.dir") + "/measurements/node"
                + getReceiver().getPartitionId());
        if (!logDirectory.exists()) {
            if (!logDirectory.mkdirs()) {
                throw new RuntimeException("Cannot create log dir " + logDirectory.getAbsolutePath());
            }
        }
        CsvReporter.enable(logDirectory, 10, TimeUnit.SECONDS);
        ConsoleReporter.enable(10, TimeUnit.SECONDS);

        SimplePE1 simplePE1 = createPE(SimplePE1.class, "simplePE1");
        ZkClient zkClient = new ZkClient(zkString);
        zkClient.waitUntilExists("/benchmarkConfig/warmupIterations", TimeUnit.SECONDS, 60);

        // TODO fix hardcoded cluster name (pass injector config?)
        int nbInjectors = zkClient.countChildren("/s4/clusters/testCluster1/tasks");
        simplePE1.setNbInjectors(nbInjectors);

        createInputStream("inputStream", new KeyFinder<Event>() {

            @Override
            public List<String> get(Event event) {
                return ImmutableList.of(event.get("key"));
            }
        }, simplePE1).setParallelism(3);

        SimplePE2 simplePE2 = createPE(SimplePE2.class, "simplePE2");

        createInputStream("inputStream2", new KeyFinder<Event>() {

            @Override
            public List<String> get(Event event) {
                return ImmutableList.of(event.get("key"));
            }
        }, simplePE2);

    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    public String getZkString() {
        return zkString;
    }

}
