package io.s4.comm;

import com.google.inject.Inject;

import io.s4.core.Event;
import io.s4.core.Hasher;
import io.s4.serialize.SerializerDeserializer;

public class Sender {
    final private Emitter emitter;
    final private SerializerDeserializer serDeser;
    final private Hasher hasher;
    
    @Inject
    public Sender(Emitter emitter, SerializerDeserializer serDeser, Hasher hasher) {
        this.emitter = emitter;
        this.serDeser = serDeser;
        this.hasher = hasher;
    }
    
    public void send(String hashKey, Event event) {
        //System.out.println("SenderNonParam: Sending event " + event);
        int partition = (int) (hasher.hash(hashKey)%emitter.getPartitionCount());
        send(partition, event);
    }
     
    public void send(int partition, Event event) {
        // serialize and send
        byte[] blob = serDeser.serialize(event);
        emitter.send(partition, blob);
    }
    
    public void send(Event event) {
        // send to all partitions
        byte[] blob = serDeser.serialize(event);
        for (int i = 0; i < emitter.getPartitionCount(); i++) {
            emitter.send(i, blob);
        }
    }
}
