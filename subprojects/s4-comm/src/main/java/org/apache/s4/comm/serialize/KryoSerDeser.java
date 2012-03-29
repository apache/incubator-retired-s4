package org.apache.s4.comm.serialize;

import java.nio.ByteBuffer;

import org.apache.s4.base.SerializerDeserializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.esotericsoftware.kryo.serialize.ClassSerializer;
import com.esotericsoftware.kryo.serialize.SimpleSerializer;

public class KryoSerDeser implements SerializerDeserializer {

    private Kryo kryo = new Kryo();

    private int initialBufferSize = 2048;
    private int maxBufferSize = 256 * 1024;

    public void setInitialBufferSize(int initialBufferSize) {
        this.initialBufferSize = initialBufferSize;
    }

    public void setMaxBufferSize(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public KryoSerDeser() {
        this(Thread.currentThread().getContextClassLoader());
    }

    /**
     * 
     * @param classLoader
     *            classloader able to handle classes to serialize/deserialize. For instance, application-level events
     *            can only be handled by the application classloader.
     */
    public KryoSerDeser(ClassLoader classLoader) {
        kryo.setClassLoader(classLoader);
        kryo.setRegistrationOptional(true);

        kryo.register(Class.class, new ClassSerializer(kryo));
        // UUIDs don't have a no-arg constructor.
        kryo.register(java.util.UUID.class, new SimpleSerializer<java.util.UUID>() {
            @Override
            public java.util.UUID read(ByteBuffer buf) {
                return new java.util.UUID(buf.getLong(), buf.getLong());
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
