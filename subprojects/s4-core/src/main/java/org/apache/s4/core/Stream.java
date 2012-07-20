package org.apache.s4.core;

import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.s4.base.Event;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.GenericKeyFinder;
import org.apache.s4.base.KeyFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * {@link Stream} and {@link ProcessingElement} objects represent the links and nodes in the application graph. A stream
 * sends an {@link Event} object to {@link ProcessingElement} instances located anywhere in a cluster.
 * <p>
 * Once a stream is instantiated, it is immutable.
 * <p>
 * To build an application create stream objects using use the {@link StreamFactory} class.
 */
public class Stream<T extends Event> implements Runnable, Streamable {

    private static final Logger logger = LoggerFactory.getLogger(Stream.class);

    final static private String DEFAULT_SEPARATOR = "^";
    final static private int CAPACITY = 1000;
    private static int idCounter = 0;
    private String name;
    protected Key<T> key;
    private ProcessingElement[] targetPEs;
    protected final BlockingQueue<EventMessage> queue = new ArrayBlockingQueue<EventMessage>(CAPACITY);
    private Thread thread;
    final private Sender sender;
    final private Receiver receiver;
    // final private int id;
    final private App app;
    private Class<T> eventType = null;

    /**
     * Send events using a {@link KeyFinder<T>}. The key finder extracts the value of the key which is used to determine
     * the target {@link org.apache.s4.comm.topology.ClusterNode} for an event.
     * 
     * @param app
     *            we always register streams with the parent application.
     */
    public Stream(App app) {
        this.app = app;
        this.sender = app.getSender();
        this.receiver = app.getReceiver();
    }

    public void start() {

        if (logger.isTraceEnabled()) {
            if (targetPEs != null) {
                for (ProcessingElement pe : targetPEs) {
                    logger.trace("Starting stream [{}] with target PE [{}].", this.getName(), pe.getName());
                }
            }
        }

        /* Start streaming. */
        thread = new Thread(this, name);
        thread.setContextClassLoader(getApp().getClass().getClassLoader());
        thread.start();
        this.receiver.addStream(this);
    }

    /**
     * Name the stream.
     * 
     * @param name
     *            the stream name, default is an empty string.
     * @return the stream object
     */
    public Stream<T> setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * Define the key finder for this stream.
     * 
     * @param keyFinder
     *            a function to lookup the value of the key.
     * @return the stream object
     */
    public Stream<T> setKey(KeyFinder<T> keyFinder) {
        if (keyFinder == null) {
            this.key = null;
        } else {
            this.key = new Key<T>(keyFinder, DEFAULT_SEPARATOR);
        }
        return this;
    }

    Stream<T> setEventType(Class<T> type) {
        this.eventType = type;
        return this;
    }

    /**
     * Define the key finder for this stream using a descriptor.
     * 
     * @param keyFinderString
     *            a descriptor to lookup up the value of the key.
     * @return the stream object
     */
    public Stream<T> setKey(String keyName) {

        Preconditions.checkNotNull(eventType);

        KeyFinder<T> kf = new GenericKeyFinder<T>(keyName, eventType);
        setKey(kf);

        return this;
    }

    /**
     * Send events from this stream to a PE.
     * 
     * @param pe
     *            a target PE.
     * 
     * @return the stream object
     */
    public Stream<T> setPE(ProcessingElement pe) {
        app.addStream(this);
        return this;
    }

    /**
     * Send events from this stream to various PEs.
     * 
     * @param pe
     *            a target PE array.
     * 
     * @return the stream object
     */
    public Stream<T> setPEs(ProcessingElement[] pes) {
        this.targetPEs = pes;
        return this;
    }

    /**
     * Sends an event.
     * 
     * @param event
     */
    @SuppressWarnings("unchecked")
    public void put(Event event) {
        try {
            event.setStreamId(getName());
            event.setAppId(app.getId());

            /*
             * Events may be sent to local or remote partitions or both. The following code implements the logic.
             */
            if (key != null) {

                /*
                 * We send to a specific PE instance using the key but we don't know if the target partition is remote
                 * or local. We need to ask the sender.
                 */
                if (!sender.checkAndSendIfNotLocal(key.get((T) event), event)) {

                    /*
                     * Sender checked and decided that the target is local so we simply put the event in the queue and
                     * we save the trip over the network.
                     */
                    queue.put(new EventMessage(String.valueOf(event.getAppId()), event.getStreamName(), app
                            .getSerDeser().serialize(event)));
                }

            } else {

                /*
                 * We are broadcasting this event to all PE instance. In a cluster, we need to send the event to every
                 * node. The sender method takes care of the remote partitions an we take care of putting the event into
                 * the queue.
                 */
                sender.sendToRemotePartitions(event);
                queue.put(new EventMessage(String.valueOf(event.getAppId()), event.getStreamName(), app.getSerDeser()
                        .serialize(event)));
            }
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting to put an event in the queue: {}.", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Implements the {@link ReceiverListener} interface. The low level {@link Receiver} object call this method when a
     * new {@link Event} is available.
     */
    public void receiveEvent(EventMessage event) {
        try {
            queue.put(event);
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting to put an event in the queue: {}.", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the key
     */
    public Key<T> getKey() {
        return key;
    }

    /**
     * @return the stream id
     */
    // int getId() {
    // return id;
    // }

    /**
     * @return the app
     */
    public App getApp() {
        return app;
    }

    /**
     * @return the list of target processing element prototypes.
     */
    public ProcessingElement[] getTargetPEs() {
        return targetPEs;
    }

    /**
     * Stop and close this stream.
     */
    public void close() {
        thread.interrupt();
    }

    /**
     * @return the sender object
     */
    public Sender getSender() {
        return sender;
    }

    /**
     * @return the receiver object
     */
    public Receiver getReceiver() {
        return receiver;
    }

    @Override
    public void run() {
        while (true) {
            try {
                /* Get oldest event in queue. */
                EventMessage eventMessage = queue.take();

                @SuppressWarnings("unchecked")
                T event = (T) app.getSerDeser().deserialize(eventMessage.getSerializedEvent());

                /* Send event to each target PE. */
                for (int i = 0; i < targetPEs.length; i++) {

                    if (key == null) {

                        /* Broadcast to all PE instances! */

                        /* STEP 1: find all PE instances. */

                        Collection<ProcessingElement> pes = targetPEs[i].getInstances();

                        /* STEP 2: iterate and pass event to PE instance. */
                        for (ProcessingElement pe : pes) {

                            pe.handleInputEvent(event);
                        }

                    } else {

                        /* We have a key, send to target PE. */

                        /* STEP 1: find the PE instance for key. */
                        ProcessingElement pe = targetPEs[i].getInstanceForKey(key.get(event));

                        /* STEP 2: pass event to PE instance. */
                        pe.handleInputEvent(event);
                    }
                }

            } catch (InterruptedException e) {
                logger.info("Closing stream {}.", name);
                receiver.removeStream(this);
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    public Stream<T> register() {
        app.addStream(this);
        return this;
    }
}
