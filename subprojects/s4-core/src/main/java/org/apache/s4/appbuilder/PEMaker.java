package org.apache.s4.appbuilder;

import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;

public class PEMaker {

    private Class<? extends ProcessingElement> type;

    /* Only package classes can instantiate this class. */
    PEMaker(Class<? extends ProcessingElement> type) {
        this.type = type;
    }

    public PEMaker withTrigger(Class<? extends Event> eventType, int numEvents, long interval, TimeUnit timeUnit) {

        return this;
    }

    public PEMaker withTimerInterval(long interval, TimeUnit timeUnit) {

        return this;
    }

    public <T extends Event> PEMaker to(StreamMaker stream) {
        return this;
    }

    public ProcessingElement make() {
        return null;
    }
}
