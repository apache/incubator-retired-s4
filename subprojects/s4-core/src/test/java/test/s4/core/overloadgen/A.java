package test.s4.core.overloadgen;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;


public class A extends ProcessingElement {
    
    public Class<? extends Event> processedEventClass;
    public Class<? extends Event> processedTriggerEventClass;
    boolean processedTriggerThroughGenericMethod = false;
    
    public void onEvent(Event event) {
        processedEventClass = Event.class;
    }
        
    public void onEvent(Event2 event) {
        processedEventClass = event.getClass();
    }
    
    public void onEvent(Event1 event) {
        processedEventClass = event.getClass();
    }

    public void onEvent(Event1a event) {
        processedEventClass = event.getClass();
    }
    
    public void onTrigger(Event event) {
        processedTriggerEventClass = event.getClass();
        processedTriggerThroughGenericMethod = true;
    }
    
    public void onTrigger(Event1 event ) {
        processedTriggerEventClass = event.getClass();
        processedTriggerThroughGenericMethod = false;
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
