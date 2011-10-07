package io.s4.serialize;

import io.s4.base.SerializerDeserializer;

import java.nio.ByteBuffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;

public class KryoSerDeser implements SerializerDeserializer {

    private Kryo kryo = new Kryo();
    
    private int initialBufferSize = 2048;
    private int maxBufferSize = 256*1024;

    public void setInitialBufferSize(int initialBufferSize) {
        this.initialBufferSize = initialBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public KryoSerDeser() {
        kryo.setRegistrationOptional(true);

        // UUIDs don't have a no-arg constructor.
        kryo.register(java.util.UUID.class,
                      new SimpleSerializer<java.util.UUID>() {
                          @Override
                          public java.util.UUID read(ByteBuffer buf) {
                              return new java.util.UUID(buf.getLong(),
                                                        buf.getLong());
                          }

                          @Override
                          public void write(ByteBuffer buf, java.util.UUID uuid) {
                              buf.putLong(uuid.getMostSignificantBits());
                              buf.putLong(uuid.getLeastSignificantBits());
                          }

                      });
    }

    @Override
    public Object deserialize(byte[] rawMessage) {
        ObjectBuffer buffer = new ObjectBuffer(kryo, initialBufferSize, maxBufferSize);
        return buffer.readClassAndObject(rawMessage);
    }

    @Override
    public byte[] serialize(Object message) {
        ObjectBuffer buffer = new ObjectBuffer(kryo, initialBufferSize, maxBufferSize);
        return buffer.writeClassAndObject(message);
    }
}