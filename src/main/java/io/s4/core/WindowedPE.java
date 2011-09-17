package io.s4.core;

import java.util.Collection;

import org.apache.commons.collections15.buffer.CircularFifoBuffer;

public abstract class WindowedPE<T> extends ProcessingElement {

    final private int bufferSize;
    private CircularFifoBuffer<T> circularBuffer;

    WindowedPE(App app, int bufferSize) {
        super(app);
        this.bufferSize = bufferSize;
    }

    abstract protected void processInputEvent(Event event);

    abstract public void processOutputEvent(Event event);

    abstract protected void onCreate();

    abstract protected void onRemove();

    protected Collection<T> getBuffer() {

        if (circularBuffer == null) {
            circularBuffer = new CircularFifoBuffer<T>(bufferSize);
        }
        return circularBuffer;
    }
}
