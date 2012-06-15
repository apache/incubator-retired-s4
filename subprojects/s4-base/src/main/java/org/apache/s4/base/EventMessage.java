package org.apache.s4.base;

/**
 * 
 * Encapsulates application-level events of type {@link Event}.
 * 
 * Indeed, events that are defined at the application level can only be handled by the classloader of the corresponding
 * application.
 * 
 * Includes routing information (application name, stream name), so that this message can be dispatched at the
 * communication level.
 * 
 * 
 */
public class EventMessage {

    private String appName;
    private String streamName;
    private byte[] serializedEvent;

    public EventMessage() {
    }

    /**
     * 
     * @param appName
     *            name of the application
     * @param streamName
     *            name of the stream
     * @param serializedEvent
     *            application-specific {@link Event} instance in serialized form
     */
    public EventMessage(String appName, String streamName, byte[] serializedEvent) {
        super();
        this.appName = appName;
        this.streamName = streamName;
        this.serializedEvent = serializedEvent;
    }

    public String getAppName() {
        return appName;
    }

    public String getStreamName() {
        return streamName;
    }

    public byte[] getSerializedEvent() {
        return serializedEvent;
    }

}
