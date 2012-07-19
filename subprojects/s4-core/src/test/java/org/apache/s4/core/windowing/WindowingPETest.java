package org.apache.s4.core.windowing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.apache.s4.core.window.AbstractSlidingWindowPE;
import org.apache.s4.core.window.DefaultAggregatingSlot;
import org.apache.s4.core.window.DefaultAggregatingSlot.DefaultAggregatingSlotFactory;
import org.apache.s4.fixtures.MockCommModule;
import org.apache.s4.fixtures.MockCoreModule;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class WindowingPETest {

    public static final long NB_EVENTS = 1000000;
    public static final CountDownLatch signalAllEventsProcessed = new CountDownLatch(1);
    public static final List<Integer> allValues = new ArrayList<Integer>();

    private static final String STREAM_NAME = "stream1";
    private static final String APP_NAME = "app1";

    @Test
    public void test() {
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.DEBUG);
        Injector injector = Guice.createInjector(new MockCommModule(), new MockCoreModule());
        TestTimeWindowedApp app = injector.getInstance(TestTimeWindowedApp.class);
        app.init();
        app.start();

        for (int i = 0; i < NB_EVENTS; i++) {
            Event e = new Event();
            e.put("value", Integer.class, i);
            app.stream1.receiveEvent(new EventMessage(APP_NAME, STREAM_NAME, app.getSerDeser().serialize(e)));
        }

        try {
            Assert.assertTrue(signalAllEventsProcessed.await(30, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Assert.fail();
        }
        Assert.assertEquals(NB_EVENTS, allValues.size());
        for (int i = 0; i < NB_EVENTS; i++) {
            Assert.assertEquals((Integer) i, allValues.get(i));
        }
    }

    public static class TestTimeWindowedApp extends App {

        private Stream<Event> stream1;

        @Override
        protected void onStart() {
            // TODO Auto-generated method stub

        }

        @Override
        protected void onInit() {
            AbstractSlidingWindowPE<DefaultAggregatingSlot<Integer>, Integer, List<Integer>> wPE1 = createSlidingWindowPE(
                    WindowingPE1.class, 10L, TimeUnit.MILLISECONDS, 100000,
                    new DefaultAggregatingSlotFactory<Integer>());
            wPE1.setTimerInterval(10, TimeUnit.MILLISECONDS);
            stream1 = createStream(STREAM_NAME, new KeyFinder<Event>() {

                @Override
                public List<String> get(final Event event) {
                    return ImmutableList.of("X");
                }
            }, wPE1);

        }

        @Override
        protected void onClose() {
            // TODO Auto-generated method stub

        }

    }

}
