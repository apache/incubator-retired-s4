package org.apache.s4.benchmark.simpleApp1;

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

public class SimpleApp extends App {

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
                + getReceiver().getPartition());
        if (!logDirectory.exists()) {
            if (!logDirectory.mkdirs()) {
                throw new RuntimeException("Cannot create log dir " + logDirectory.getAbsolutePath());
            }
        }
        CsvReporter.enable(logDirectory, 5, TimeUnit.SECONDS);
        ConsoleReporter.enable(30, TimeUnit.SECONDS);

        SimplePE1 simplePE1 = createPE(SimplePE1.class, "simplePE1");
        ZkClient zkClient = new ZkClient(zkString);
        zkClient.waitUntilExists("/benchmarkConfig/warmupIterations", TimeUnit.SECONDS, 60);
        Long warmupIterations = zkClient.readData("/benchmarkConfig/warmupIterations");
        Long testIterations = zkClient.readData("/benchmarkConfig/testIterations");

        // TODO fix hardcoded cluster name (pass injector config?)
        int nbInjectors = zkClient.countChildren("/s4/clusters/testCluster1/tasks");
        simplePE1.setNbInjectors(nbInjectors);

        simplePE1.setWarmupIterations(warmupIterations);
        simplePE1.setTestIterations(testIterations);
        createInputStream("inputStream", new KeyFinder<Event>() {

            @Override
            public List<String> get(Event event) {
                return ImmutableList.of(event.get("key"));
            }
        }, simplePE1);

    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    public String getZkString() {
        return zkString;
    }

}
