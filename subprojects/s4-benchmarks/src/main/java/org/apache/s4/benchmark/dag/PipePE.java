package org.apache.s4.benchmark.dag;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipePE extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(PipePE.class);

    private Stream<Event> downstream;

    public void onEvent(Event event) {

        String value = event.get("value", String.class);
        logger.trace("PipePE : {} -> {}", getId(), value);
        Event outputEvent = new Event();
        // if we reuse the same key, with the same key finder, this event goes to the current node
        outputEvent.put("key", int.class, event.get("key", int.class));
        outputEvent.put("value", String.class, value + "->" + getId());
        downstream.put(outputEvent);
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    public void setDownstream(Stream<Event> downstream) {
        this.downstream = downstream;
    }

}
