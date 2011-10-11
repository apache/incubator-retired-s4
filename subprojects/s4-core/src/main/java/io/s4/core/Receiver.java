package io.s4.core;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapMaker;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import io.s4.base.Listener;
import io.s4.base.SerializerDeserializer;
import io.s4.base.Event;
import io.s4.core.Sender;

/**
 * The {@link Receiver} and its counterpart {@link Sender} are the top level
 * classes of the communication layer.
 * <p>
 * {@link Receiver} is responsible for receiving an event to a
 * {@link ProcessingElement} instance using a hashKey.
 * <p>
 * A Listener implementation receives data from the network and passes an event
 * as a byte array to the {@link Receiver}. The byte array is de-serialized and
 * converted into an {@link Event}. Finally the event is passed to the matching
 * streams. There is a single {@link Receiver} instance per node.
 * 
 * Details on how the cluster is partitioned and how events are serialized and
 * transmitted to its destination are hidden from the application developer.
 */
@Singleton
public class Receiver implements Runnable {

    private static final Logger logger = LoggerFactory
            .getLogger(Receiver.class);

    final private Listener listener;
    final private SerializerDeserializer serDeser;
    private Map<Integer, Map<Integer, Stream<? extends Event>>> streams;

    @Inject
    public Receiver(Listener listener, SerializerDeserializer serDeser) {
        this.listener = listener;
        this.serDeser = serDeser;
        new Thread(this).start();

        streams = new MapMaker().makeMap();
    }

    /** Save stream keyed by app id and stream id. */
    void addStream(Stream<? extends Event> stream) {
        int appId = stream.getApp().getId();
        Map<Integer, Stream<? extends Event>> appMap = streams.get(appId);
        if (appMap == null) {
            appMap = new MapMaker().makeMap();
            streams.put(appId, appMap);
        }
        appMap.put(stream.getId(), stream);
    }

    /** Remove stream when it is no longer needed. */
    void removeStream(Stream<? extends Event> stream) {
        int appId = stream.getApp().getId();
        Map<Integer, Stream<? extends Event>> appMap = streams.get(appId);
        if (appMap == null) {
            logger.error("Tried to remove a stream that is not registered in the receiver.");
            return;
        }
        appMap.remove(stream.getId());
    }

    public void run() {
        while (!Thread.interrupted()) {
            byte[] message = listener.recv();
            Event event = (Event) serDeser.deserialize(message);

            int appId = event.getAppId();
            int streamId = event.getStreamId();

            /*
             * Match appId and streamId in event to the target stream and pass
             * the event to the target stream. TODO: make this more efficient
             * for the case in which we send the same event to multiple PEs.
             */
            try {
                streams.get(appId).get(streamId).receiveEvent(event);
            } catch (NullPointerException e) {
                logger.error(
                        "Could not find target stream for event with appId={} and streamId={}",
                        appId, streamId);
            }
        }
    }
}
