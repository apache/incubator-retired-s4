package org.apache.s4.core;

import org.apache.s4.base.Hasher;
import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.base.SerializerDeserializer;

import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class RemoteSender extends Sender {

    @Inject
    public RemoteSender(RemoteEmitter emitter, SerializerDeserializer serDeser, Hasher hasher) {
        super(emitter, serDeser, hasher);
    }

}
