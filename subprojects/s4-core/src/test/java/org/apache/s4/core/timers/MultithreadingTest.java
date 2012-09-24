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

package org.apache.s4.core.timers;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.AppModule;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.apache.s4.fixtures.MockCommModule;
import org.apache.s4.fixtures.MockCoreModule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class MultithreadingTest {
    private static Logger logger = LoggerFactory.getLogger(MultithreadingTest.class);

    private static final String STREAM_NAME = "StreamName";
    private static final String APP_NAME = "AppName";

    /*
     * We inject one event and fire one onTime() event, both should be synchronized (not running in parallel)
     */
    @Test
    public void testSynchronization() throws IOException, InterruptedException {
        Injector injector = Guice.createInjector(new MockCommModule(), new MockCoreModule(), new AppModule(getClass()
                .getClassLoader()));
        TestApp app = injector.getInstance(TestApp.class);
        app.count = 2; // One for the event, another for the timer
        app.init();
        app.start();

        Event event = new Event();
        event.setStreamId(STREAM_NAME);
        app.testStream.receiveEvent(event);

        /*
         * This must raise a timeout, since the onTime() event is blocked waiting for the onEvent() call to finish. If
         * it completes before the timeout, it means onEvent() and onTime() weren't synchronized
         */
        assertFalse("The timer wasn't synchronized with the event", app.latch.await(2, TimeUnit.SECONDS));
    }

    public static class CountdownPE extends ProcessingElement {
        CountDownLatch latch;

        public CountdownPE(App app) {
            super(app);
        }

        public void onEvent(Event ev) {
            logger.debug("Event processed");
            latch.countDown();
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                // ignore
            }
        }

        @Override
        protected void onTime() {
            logger.debug("Timer fired");
            latch.countDown();
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                // ignore
            }
        }

        @Override
        protected void onCreate() {
        }

        @Override
        protected void onRemove() {
        }
    }

    private static class TestApp extends App {
        Stream<Event> testStream;
        CountDownLatch latch;
        int count;

        @Override
        protected void onStart() {
        }

        @Override
        protected void onInit() {
            latch = new CountDownLatch(count);

            CountdownPE countdownPE = createPE(CountdownPE.class);
            countdownPE.latch = latch;
            testStream = createStream(STREAM_NAME, new KeyFinder<Event>() {
                @Override
                public List<String> get(Event event) {
                    return ImmutableList.of(Integer.toString(event.hashCode()));
                }
            }, countdownPE);
            countdownPE.setTimerInterval(1, TimeUnit.SECONDS);
        }

        @Override
        protected void onClose() {
        }

    }
}
