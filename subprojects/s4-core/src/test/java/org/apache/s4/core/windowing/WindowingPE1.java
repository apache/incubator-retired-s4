package org.apache.s4.core.windowing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.window.DefaultAggregatingSlot;
import org.apache.s4.core.window.Slot;
import org.apache.s4.core.window.WindowingPE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WindowingPE1 extends WindowingPE<Slot<Integer, List<Integer>>> {

    private static Logger logger = LoggerFactory.getLogger(WindowingPE1.class);
    AtomicInteger counter = new AtomicInteger();

    public WindowingPE1(App app, int numSlots) {
        super(app, numSlots);
    }

    @Override
    protected Slot<Integer, List<Integer>> addPeriodicSlot() {
        return new DefaultAggregatingSlot<Integer, List<Integer>>();
    }

    public void onEvent(Event event) {

        Integer value = event.get("value", Integer.class);
        currentSlot.addData(value);
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
            List<Integer> values = new ArrayList<Integer>();

            for (Slot<Integer, List<Integer>> slot : getSlots()) {
                values.addAll(slot.getAggregatedData());
            }
            // System.out.println(Arrays.toString(values.toArray(new Integer[] {})));
            WindowingPETest.allValues.addAll(values);
            WindowingPETest.signalAllEventsProcessed.countDown();
        }

    }

    public WindowingPE1(App app, long slotDuration, TimeUnit timeUnit, int numSlots) {
        super(app, slotDuration, timeUnit, numSlots);
        // TODO Auto-generated constructor stub
    }

}
