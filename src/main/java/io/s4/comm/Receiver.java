package io.s4.comm;

import java.util.HashSet;
import java.util.Set;

import com.google.inject.Inject;

import io.s4.core.Event;
import io.s4.serialize.SerializerDeserializer;

/**
 * The {@link Sender} and its counterpart {@link Receiver} are the top level
 * classes of the communication layer. 
 * <p>
 * {@link Receiver} is responsible for receiving an event to a
 * {@link ProcessingElement} instance using a hashKey.
 * <p>
 * Details on how the cluster is partitioned and how events are serialized and
 * transmitted to its destination are hidden from the application developer.
 */
public class Receiver implements Runnable {
    private Listener listener;
    private SerializerDeserializer serDeser;
    private Set<ReceiverListener> listeners = new HashSet<ReceiverListener>();
    
    @Inject
    public Receiver(Listener listener, SerializerDeserializer serDeser) {
        this.listener = listener;
        this.serDeser = serDeser;
        new Thread(this).start();
    }
    
    public void addListener(ReceiverListener listener) {
        listeners.add(listener);
    }
    
    public void removeListener(ReceiverListener listener) {
        listeners.remove(listener);
    }
    
    public void run() {
        while (!Thread.interrupted()) {
            //System.out.println("ReceiverNonParam: About to wait to receive message");
            byte[] message = listener.recv();
            Object event = serDeser.deserialize(message);
            //System.out.println("ReceiverNonParam: Receiving event " + event);
            
            for (ReceiverListener listener : listeners) {
                //System.out.println("ReceiverNonParam: Calling receiver listener " + listener);
                listener.receiveEvent((Event)event);
            }
        }   
    }
}
