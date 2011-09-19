package io.s4.comm;

public interface ReceiverListener<T> {
    public void receiveEvent(T event);
}
