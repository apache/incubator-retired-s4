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
package org.apache.s4.benchmark.dag;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.comm.topology.ZkClient;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.CsvReporter;

public class DagApp extends App {

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

        FirstPE inputPE = createPE(FirstPE.class, "firstPE");
        ZkClient zkClient = new ZkClient(zkString);
        zkClient.waitUntilExists("/benchmarkConfig/warmupIterations", TimeUnit.SECONDS, 60);

        // TODO fix hardcoded cluster name (pass injector config?)
        int nbInjectors = zkClient.countChildren("/s4/clusters/testCluster1/tasks");

        createInputStream("inputStream", new KeyFinder<Event>() {

            @Override
            public List<String> get(Event event) {
                return ImmutableList.of(String.valueOf(event.get("key")));
            }
        }, inputPE).setParallelism(1);

        PipePE pe1 = createPE(PipePE.class, "pe1");

        Stream<Event> pipe = createStream("firstPE->pe1", new KeyFinder<Event>() {

            @Override
            public List<String> get(Event event) {
                return ImmutableList.of(String.valueOf(event.get("key")));
            }
        }, pe1).setParallelism(1);

        inputPE.setDownstream(pipe);

        PipePE pe2 = addPipePE(pe1, "pe1", "pe2");
        PipePE pe3 = addPipePE(pe2, "pe2", "pe3");
        PipePE pe4 = addPipePE(pe3, "pe3", "pe4");
        PipePE pe5 = addPipePE(pe4, "pe4", "pe5");
        PipePE pe6 = addPipePE(pe5, "pe5", "pe6");
        PipePE pe7 = addPipePE(pe6, "pe6", "pe7");
        PipePE pe8 = addPipePE(pe7, "pe7", "pe8");
        PipePE pe9 = addPipePE(pe8, "pe8", "pe9");

        LastPE endPE = createPE(LastPE.class, "endPE");
        Stream<Event> endStream = createStream("pe9->endPE", endPE);
        pe9.setDownstream(endStream);

    }

    private PipePE addPipePE(PipePE upstreamPE, String upstreamPEName, String peName) {
        PipePE pe = createPE(PipePE.class, peName);

        Stream<Event> pipe = createStream(upstreamPEName + "->" + peName, new KeyFinder<Event>() {

            @Override
            public List<String> get(Event event) {
                return ImmutableList.of(String.valueOf(event.get("key")));
            }
        }, pe).setParallelism(1);

        upstreamPE.setDownstream(pipe);
        return pe;
    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    public String getZkString() {
        return zkString;
    }

}
