package org.apache.s4.core.windowing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.window.AbstractSlidingWindowPE;
import org.apache.s4.core.window.DefaultAggregatingSlot;
import org.apache.s4.core.window.SlotFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowingPE1 extends AbstractSlidingWindowPE<DefaultAggregatingSlot<Integer>, Integer, List<Integer>> {

    private static Logger logger = LoggerFactory.getLogger(WindowingPE1.class);
    AtomicInteger counter = new AtomicInteger();

    public WindowingPE1(App app, long slotDuration, TimeUnit timeUnit, int numSlots,
            SlotFactory<DefaultAggregatingSlot<Integer>> slotFactory) {
        super(app, slotDuration, timeUnit, numSlots, slotFactory);
    }

    public WindowingPE1(App app, int numSlots, long slotCapacity,
            SlotFactory<DefaultAggregatingSlot<Integer>> slotFactory) {
        super(app, numSlots, slotCapacity, slotFactory);
    }

    public void onEvent(Event event) {

        Integer value = event.get("value", Integer.class);
        updateOpenSlot(value);
        counter.incrementAndGet();
        if (counter.get() % 1000 == 0) {
            logger.trace("received value [{}]", event.get("value", Integer.class));
        }
    }

    @Override
    protected void onRemove() {

    }

    @Override
    protected void onTime() {
        if (counter.get() == WindowingPETest.NB_EVENTS) {
            // System.out.println(Arrays.toString(values.toArray(new Integer[] {})));
            WindowingPETest.allValues.addAll(evaluateWindow(getSlots()));
            WindowingPETest.signalAllEventsProcessed.countDown();
        }

    }

    @Override
    protected List<Integer> evaluateWindow(Collection<DefaultAggregatingSlot<Integer>> slots) {
        List<Integer> values = new ArrayList<Integer>();

        for (DefaultAggregatingSlot<Integer> slot : getSlots()) {
            values.addAll(slot.getAggregatedData());
        }
        return values;
    }

}
