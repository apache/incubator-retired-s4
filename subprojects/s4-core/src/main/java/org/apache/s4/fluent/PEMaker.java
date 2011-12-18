package org.apache.s4.fluent;

import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Helper class to add a processing element to an S4 application.
 * 
 * @see example {@link S4Maker}
 * 
 */
public class PEMaker {

    private static final Logger logger = LoggerFactory.getLogger(PEMaker.class);

    final private Class<? extends ProcessingElement> type;
    final private AppMaker app;
    private ProcessingElement pe = null;

    private long timerInterval = 0;

    private long triggerInterval = 0;
    private Class<? extends Event> triggerEventType = null;
    private int triggerNumEvents = 0;

    private int cacheMaximumSize = 0;
    private long cacheDuration = 0;

    private PropertiesConfiguration properties = new PropertiesConfiguration();

    private boolean isSingleton = false;

    PEMaker(AppMaker app, Class<? extends ProcessingElement> type) {
        Preconditions.checkNotNull(type);
        this.type = type;
        this.app = app;
        app.add(this, null);
    }

    /**
     * Configure the PE expiration and cache size.
     * <p>
     * PE instances will be automatically removed from the cache once a fixed duration has elapsed after the PEs
     * creation, or last access.
     * <p>
     * Least accessed PEs will automatically be removed from the cache when the number of PEs approaches maximumSize.
     * <p>
     * When this method is called all existing PE instances are destroyed.
     * 
     * 
     * @param maximumSize
     *            the approximate maximum number of PEs in the cache.
     * @param duration
     *            the PE duration
     * @param timeUnit
     *            the time unit
     * @return the PEMaker
     */
    public CacheMaker addCache() {

        return new CacheMaker();
    }

    /**
     * Configure a trigger that is fired when the following conditions occur:
     * 
     * <ul>
     * <li>An event of eventType arrived to the PE instance
     * <li>numEvents have arrived since the last time this trigger was fired -OR- time since last event is greater than
     * interval.
     * </ul>
     * 
     * <p>
     * When the trigger fires, the method <tt>trigger(EventType event)</tt> is called. Where <tt>EventType</tt> matches
     * the argument eventType.
     * 
     * @param eventType
     *            the type of event on which this trigger will fire.
     * @param numEvents
     *            number of events since last trigger activation. Must be greater than zero. (Set to one to trigger on
     *            every input event.)
     * @param interval
     *            minimum time between triggers. Set to zero if no time interval needed.
     * @param timeUnit
     *            the TimeUnit for the argument interval. Can set to null if no time interval needed.
     * @return the PEMaker
     */
    public TriggerMaker addTrigger() {

        return new TriggerMaker();
    }

    /**
     * Set a timer that calls {@link ProcessingElement#onTime()}.
     * 
     * If {@code interval==0} the timer is disabled.
     * 
     * @param interval
     *            in timeUnit
     * @param timeUnit
     *            the timeUnit of interval
     * @return the PEMaker
     */
    public TimerMaker addTimer() {
        return new TimerMaker();
    }

    public StreamMaker emit(Class<? extends Event> type) {

        logger.debug("PE [{}] emits event of type [{}].", this.getType().getName(), type.getCanonicalName());
        StreamMaker stream = new StreamMaker(app, type);
        app.add(this, stream);
        return stream;
    }

    public PEMaker withKey(String key) {

        return this;
    }

    public PEMaker with(String key, Object value) {

        properties.addProperty(key, value);
        return this;
    }

    /**
     * @return the timerInterval
     */
    long getTimerInterval() {
        return timerInterval;
    }

    /**
     * @return the triggerInterval
     */
    long getTriggerInterval() {
        return triggerInterval;
    }

    /**
     * @return the triggerEventType
     */
    Class<? extends Event> getTriggerEventType() {
        return triggerEventType;
    }

    /**
     * @return the triggerNumEvents
     */
    int getTriggerNumEvents() {
        return triggerNumEvents;
    }

    /**
     * @return the cacheMaximumSize
     */
    int getCacheMaximumSize() {
        return cacheMaximumSize;
    }

    /**
     * @return the cacheDuration
     */
    long getCacheDuration() {
        return cacheDuration;
    }

    /**
     * @return the type
     */
    Class<? extends ProcessingElement> getType() {
        return type;
    }

    /**
     * @return the properties
     */
    PropertiesConfiguration getProperties() {
        return properties;
    }

    /**
     * @return the pe
     */
    public ProcessingElement getPe() {
        return pe;
    }

    /**
     * @param pe
     *            the pe to set
     */
    public void setPe(ProcessingElement pe) {
        this.pe = pe;
    }

    public class TriggerMaker {

        public TriggerMaker fireOn(Class<? extends Event> eventType) {

            triggerEventType = eventType;
            return this;
        }

        public TriggerMaker ifNumEvents(int numEvents) {

            triggerNumEvents = numEvents;
            return this;
        }

        public TriggerMaker ifInterval(long interval, TimeUnit timeUnit) {

            if (timeUnit != null)
                triggerInterval = timeUnit.convert(interval, TimeUnit.MILLISECONDS);
            return this;
        }
    }

    public class CacheMaker {

        public CacheMaker ofSize(int maxSize) {
            cacheMaximumSize = maxSize;
            return this;
        }

        public CacheMaker withDuration(long duration, TimeUnit timeUnit) {
            cacheDuration = timeUnit.convert(duration, TimeUnit.MILLISECONDS);
            return this;
        }
    }

    public class TimerMaker {

        public TimerMaker withDuration(long duration, TimeUnit timeUnit) {
            timerInterval = TimeUnit.MILLISECONDS.convert(duration, timeUnit);
            timerInterval = duration;
            return this;
        }
    }

    public PEMaker asSingleton() {
        this.isSingleton = true;
        return this;
    }

    public boolean isSingleton() {
        return isSingleton;
    }
}
