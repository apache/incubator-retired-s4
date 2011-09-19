package io.s4.serialize;

public interface SerializerDeserializer {
    public byte[] serialize(Object message);

    public Object deserialize(byte[] rawMessage);
}
