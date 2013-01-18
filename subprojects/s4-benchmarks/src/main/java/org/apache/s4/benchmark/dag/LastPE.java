package org.apache.s4.benchmark.dag;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LastPE extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(LastPE.class);

    public void onEvent(Event event) {

        String value = event.get("value", String.class);
        logger.trace("LastPE : {} -> {}", getId(), value);
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

}
