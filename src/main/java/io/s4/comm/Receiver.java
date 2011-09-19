package io.s4.comm;

import java.util.HashSet;
import java.util.Set;

import io.s4.core.Event;
import io.s4.serialize.SerializerDeserializer;

public class Receiver implements Runnable {
    private Listener listener;
    private SerializerDeserializer serDeser;
    private Set<ReceiverListenerNonParam> listeners = new HashSet<ReceiverListenerNonParam>();
    
    public Receiver(Listener listener, SerializerDeserializer serDeser) {
        this.listener = listener;
        this.serDeser = serDeser;
        new Thread(this).start();
    }
    
    public void addListener(ReceiverListenerNonParam listener) {
        listeners.add(listener);
    }
    
    public void removeListener(ReceiverListenerNonParam listener) {
        listeners.remove(listener);
    }
    
    public void run() {
        while (!Thread.interrupted()) {
            //System.out.println("ReceiverNonParam: About to wait to receive message");
            byte[] message = listener.recv();
            Object event = serDeser.deserialize(message);
            //System.out.println("ReceiverNonParam: Receiving event " + event);
            
            for (ReceiverListenerNonParam listener : listeners) {
                //System.out.println("ReceiverNonParam: Calling receiver listener " + listener);
                listener.receiveEvent((Event)event);
            }
        }   
    }
}
