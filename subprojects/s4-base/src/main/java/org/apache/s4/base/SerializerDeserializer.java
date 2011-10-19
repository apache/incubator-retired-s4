package org.apache.s4.base;

public interface SerializerDeserializer {
    public byte[] serialize(Object message);

    public Object deserialize(byte[] rawMessage);
}
