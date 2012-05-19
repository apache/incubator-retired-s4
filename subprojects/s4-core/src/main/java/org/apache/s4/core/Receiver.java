package org.apache.s4.core;

import java.util.Map;

import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * The {@link Receiver} and its counterpart {@link Sender} are the top level classes of the communication layer.
 * <p>
 * {@link Receiver} is responsible for receiving an event to a {@link ProcessingElement} instance using a hashKey.
 * <p>
 * A Listener implementation receives data from the network and passes an event as a byte array to the {@link Receiver}.
 * The byte array is de-serialized and converted into an {@link Event}. Finally the event is passed to the matching
 * streams.
 * </p>
 * There is a single {@link Receiver} instance per node.
 * 
 * Details on how the cluster is partitioned and how events are serialized and transmitted to its destination are hidden
 * from the application developer. </p>
 */
@Singleton
public class Receiver implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Receiver.class);

    final private Listener listener;
    final private SerializerDeserializer serDeser;
    private Map<Integer, Map<String, Stream<? extends Event>>> streams;
    private Thread thread;

    @Inject
    public Receiver(Listener listener, SerializerDeserializer serDeser) {
        this.listener = listener;
        this.serDeser = serDeser;

        thread = new Thread(this, "Receiver");
        thread.start();

        streams = new MapMaker().makeMap();
    }

    int getPartition() {
        return listener.getPartitionId();
    }

    /** Save stream keyed by app id and stream id. */
    void addStream(Stream<? extends Event> stream) {
        int appId = stream.getApp().getId();
        Map<String, Stream<? extends Event>> appMap = streams.get(appId);
        if (appMap == null) {
            appMap = new MapMaker().makeMap();
            streams.put(appId, appMap);
        }
        appMap.put(stream.getName(), stream);
    }

    /** Remove stream when it is no longer needed. */
    void removeStream(Stream<? extends Event> stream) {
        int appId = stream.getApp().getId();
        Map<String, Stream<? extends Event>> appMap = streams.get(appId);
        if (appMap == null) {
            logger.error("Tried to remove a stream that is not registered in the receiver.");
            return;
        }
        appMap.remove(stream.getName());
    }

    public void run() {
        // TODO: this thread never seems to get interrupted. SHould we catch an interrupted exception from listener
        // here?
        while (!Thread.interrupted()) {
            byte[] message = listener.recv();
            EventMessage event = (EventMessage) serDeser.deserialize(message);

            int appId = Integer.valueOf(event.getAppName());
            String streamId = event.getStreamName();

            /*
             * Match appId and streamId in event to the target stream and pass the event to the target stream. TODO:
             * make this more efficient for the case in which we send the same event to multiple PEs.
             */
            try {
                streams.get(appId).get(streamId).receiveEvent(event);
            } catch (NullPointerException e) {
                logger.error("Could not find target stream for event with appId={} and streamId={}", appId, streamId);
            }
        }
    }

    public void close() {
        thread.interrupt();
    }
}
