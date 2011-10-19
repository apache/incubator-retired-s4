package test.s4.core.overloadgen;

import org.apache.s4.core.ProcessingElement;

public class C extends ProcessingElement {
    
    public boolean processedEvent1Class = false;

    public void onEvent(Event1 event) {
        processedEvent1Class = true;
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
