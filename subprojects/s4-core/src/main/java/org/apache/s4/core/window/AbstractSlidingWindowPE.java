package org.apache.s4.core.window;

import java.util.Collection;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections15.buffer.CircularFifoBuffer;
import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Abstract ProcessingElement that can store historical values using a sliding window. Each set of values is called a
 * slot. Each slot represents a segment of time or a fixed number of events. Slots are consecutive in time or events.
 * 
 * Users are expected to provide a factory for creating new slots, and a method to perform a global computation on the
 * current window.
 * 
 * Slots are automatically added.
 * 
 * WHen using time-based slots, use this implementation only if you expect most slots to have values, it is not
 * efficient for sparse event streams.
 * 
 * @param <T>
 *            type of the slot implementation used for this window
 * 
 * @param <U>
 *            type of the values added to the window slots
 */
public abstract class AbstractSlidingWindowPE<T extends Slot<U>, U, V> extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(AbstractSlidingWindowPE.class);

    final private int numSlots;
    private CircularFifoBuffer<T> circularBuffer;
    final private ScheduledExecutorService windowingTimerService;
    final private long slotDurationInMilliseconds;

    private T openSlot;
    private final SlotFactory<T> slotFactory;

    private long slotCapacity = 0;
    private int eventCount = 0;

    /**
     * 
     * Constructor for the event-based slot. The abstract method {@link #addPeriodicSlot()} must be called by the
     * concrete class.
     * 
     * @param app
     *            the application
     * @param numSlots
     *            the number of slots to be stored
     */
    public AbstractSlidingWindowPE(App app, int numSlots, long slotCapacity, SlotFactory<T> slotFactory) {
        this(app, 0L, null, numSlots, slotFactory, slotCapacity);
    }

    /**
     * Constructor for time-based slots. The abstract method {@link #addPeriodicSlot()} is called periodically.
     * 
     * @param app
     *            the application
     * @param slotDuration
     *            the slot duration in timeUnit
     * @param timeUnit
     *            the unit of time
     * @param numSlots
     *            the number of slots to be stored
     */
    public AbstractSlidingWindowPE(App app, long slotDuration, TimeUnit timeUnit, int numSlots,
            SlotFactory<T> slotFactory) {
        this(app, slotDuration, timeUnit, numSlots, slotFactory, 0);

    }

    private AbstractSlidingWindowPE(App app, long slotDuration, TimeUnit timeUnit, int numSlots,
            SlotFactory<T> slotFactory, long slotCapacity) {
        super(app);
        this.numSlots = numSlots;
        this.slotFactory = slotFactory;
        this.slotCapacity = slotCapacity;
        if (slotDuration > 0l) {
            slotDurationInMilliseconds = TimeUnit.MILLISECONDS.convert(slotDuration, timeUnit);
            ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
                    .setNameFormat("SlidingWindow-" + getClass().getSimpleName()).build();
            windowingTimerService = Executors.newSingleThreadScheduledExecutor(threadFactory);

        } else {
            slotDurationInMilliseconds = 0;
            windowingTimerService = null;
        }
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    /**
     * For count-based windows, we use a trigger that adds a new slot when the current one reaches its maximum capacity.
     */
    public final void onTrigger(Event event) {
        if (windowingTimerService == null) {
            if (eventCount % slotCapacity == 0) {
                addSlot();
            }
        }
    }

    @Override
    protected void initPEPrototypeInternal() {
        super.initPEPrototypeInternal();
        windowingTimerService.scheduleAtFixedRate(new SlotTask(), slotDurationInMilliseconds,
                slotDurationInMilliseconds, TimeUnit.MILLISECONDS);
        logger.trace("TIMER: " + slotDurationInMilliseconds);

    }

    /**
     * User provided function that evaluates the whole content of the window. It must iterate across all slots. Current
     * slots are passed as a parameter and the PE instance is expected to be locked so that iteration over the slots is
     * safe.
     * 
     * @return
     */
    abstract protected V evaluateWindow(Collection<T> slots);

    /**
     * Add an object to the sliding window. Use it when the window is not periodic.
     * 
     * @param slot
     */
    protected final void addSlot() {

        if (windowingTimerService != null) {
            logger.error("Calling method addSlot() in a periodic window is not allowed.");
            return;
        }
        addNewSlot((AbstractSlidingWindowPE<T, U, V>) this);
    }

    protected void onCreate() {
        eventCount = 0;
        circularBuffer = new CircularFifoBuffer<T>(numSlots);
        if (slotDurationInMilliseconds > 0) {
            openSlot = slotFactory.createSlot();
            circularBuffer.add(openSlot);
        }
    }

    protected void updateOpenSlot(U data) {
        openSlot.update(data);
    }

    /**
     * 
     * @return the least recently inserted slot
     */
    protected T getOldestSlot() {

        return circularBuffer.get();
    }

    /** Stops the the sliding window. */
    protected void stop() {
        windowingTimerService.shutdownNow();
    }

    /**
     * 
     * @return the collection of slots
     */
    protected Collection<T> getSlots() {
        return circularBuffer;
    }

    protected T getOpenSlot() {
        return openSlot;
    }

    private class SlotTask extends TimerTask {

        @Override
        public void run() {

            logger.trace("Starting slot task");

            /* Iterate over all instances and put a new slot in the buffer. */
            for (Map.Entry<String, ProcessingElement> entry : getPEInstances().entrySet()) {
                logger.trace("pe id: " + entry.getValue().getId());
                @SuppressWarnings("unchecked")
                AbstractSlidingWindowPE<T, U, V> peInstance = (AbstractSlidingWindowPE<T, U, V>) entry.getValue();

                if (peInstance.circularBuffer == null) {
                    peInstance.circularBuffer = new CircularFifoBuffer<T>(numSlots);
                }
                addNewSlot(peInstance);
            }
        }
    }

    private void addNewSlot(AbstractSlidingWindowPE<T, U, V> peInstance) {
        synchronized (peInstance) {
            peInstance.openSlot.close();
            peInstance.openSlot = slotFactory.createSlot();
            peInstance.circularBuffer.add(peInstance.openSlot);
        }
    }
}
