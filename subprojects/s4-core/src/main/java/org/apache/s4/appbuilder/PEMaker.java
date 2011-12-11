package org.apache.s4.appbuilder;

import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;

public class PEMaker {

    private Class<? extends ProcessingElement> type;
    private AppMaker appMaker;

    /* Only package classes can instantiate this class. */
    PEMaker(AppMaker appMaker, Class<? extends ProcessingElement> type) {
        this.type = type;
        this.appMaker = appMaker;
    }

    public PEMaker withTrigger(Class<? extends Event> eventType, int numEvents, long interval, TimeUnit timeUnit) {

        return this;
    }

    public PEMaker withTimerInterval(long interval, TimeUnit timeUnit) {

        return this;
    }

    public <T extends Event> PEMaker to(StreamMaker stream) {
        appMaker.add(this, stream);
        return this;
    }

    public ProcessingElement make() {
        return null;
    }
}
