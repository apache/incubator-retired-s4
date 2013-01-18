package org.apache.s4.comm.serialize;

import org.apache.s4.base.SerializerDeserializer;

public interface SerializerDeserializerFactory {

    SerializerDeserializer createSerializerDeserializer(ClassLoader classLoader);

}
