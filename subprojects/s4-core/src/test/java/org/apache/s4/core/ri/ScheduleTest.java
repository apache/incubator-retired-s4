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

package org.apache.s4.core.ri;

import junit.framework.Assert;

import org.apache.s4.comm.topology.Cluster;
import org.apache.s4.comm.topology.ClusterChangeListener;
import org.apache.s4.comm.topology.PhysicalCluster;
import org.apache.s4.core.App;
import org.apache.s4.core.AppModule;
import org.apache.s4.fixtures.MockCommModule;
import org.apache.s4.fixtures.MockCoreModule;
import org.apache.s4.wordcount.WordClassifierPE;
import org.apache.s4.wordcount.WordCounterPE;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class ScheduleTest {
    Injector injector;

    @Before
    public void init() {
        injector = Guice.createInjector(new MockCommModule(), new MockCoreModule(), new AppModule(getClass()
                .getClassLoader()));
    }

    @Test
    public void scheduleTest() {
        TestApp app = injector.getInstance(TestApp.class);
        Cluster topology = getTestCluster(7);
        app.onInit();
        app.schedule(topology);

        Assert.assertEquals(app.count1.getPartitionCount(), 3);
        Assert.assertEquals(app.count2.getPartitionCount(), 3);
        Assert.assertEquals(app.classifier.getPartitionCount(), 1);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        app.close();
    }

    @Test(expected = RuntimeException.class)
    public void insufficientResourceTest() {
        TestApp app = injector.getInstance(TestApp.class);

        Cluster topology = getTestCluster(3);
        app.onInit();
        app.schedule(topology);
    }

    private Cluster getTestCluster(final int taskNumber) {
        return new Cluster() {

            @Override
            public PhysicalCluster getPhysicalCluster() {
                PhysicalCluster cluster = new PhysicalCluster(taskNumber);
                return cluster;
            }

            @Override
            public void addListener(ClusterChangeListener listener) {
            }

            @Override
            public void removeListener(ClusterChangeListener listener) {
            }

        };
    }

    static class TestApp extends App {
        WordCounterPE count1;
        WordCounterPE count2;
        WordClassifierPE classifier;

        @Override
        protected void onStart() {
        }

        @Override
        protected void onInit() {
            count1 = createPE(WordCounterPE.class, "count1");
            count2 = createPE(WordCounterPE.class, "count2");
            classifier = createPE(WordClassifierPE.class, "classifier");
            count2.setExclusive(3);
            classifier.setExclusive(1);
        }

        @Override
        protected void onClose() {

        }

    }
}
