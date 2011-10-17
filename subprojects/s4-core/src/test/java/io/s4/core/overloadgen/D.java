package io.s4.core.overloadgen;

import io.s4.base.Event;
import io.s4.core.ProcessingElement;

public class D extends ProcessingElement {
    
    public boolean processedGenericEvent = false;
    public boolean processedEvent1 = false;
    
    public void onEvent(Event event) {
        processedGenericEvent =true;
    }
    
    public void onEvent(Event1 event) {
        processedEvent1 = true;
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
