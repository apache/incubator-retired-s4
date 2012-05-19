package org.apache.s4.fixtures;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.apache.s4.base.RemoteEmitter;
import org.apache.s4.core.RemoteSenders;

public class ZkBasedAppModule<T> extends ZkBasedClusterManagementTestModule {
    private final Class<?> appClass;

    private Class<?> findAppClass() {
        // infer actual app class through "super type tokens" (this simple code
        // assumes actual module class is a direct subclass from this one)
        ParameterizedType pt = (ParameterizedType) getClass().getGenericSuperclass();
        Type[] fieldArgTypes = pt.getActualTypeArguments();
        return (Class<?>) fieldArgTypes[0];
    }

    protected ZkBasedAppModule() {
        super();
        this.appClass = findAppClass();
    }

    protected ZkBasedAppModule(Class<? extends Emitter> emitterClass,
            Class<? extends RemoteEmitter> remoteEmitterClass, Class<? extends Listener> listenerClass) {
        super(emitterClass, remoteEmitterClass, listenerClass);
        this.appClass = findAppClass();
    }

    @Override
    protected void configure() {
        super.configure();
        bind(appClass);
        bind(RemoteSenders.class);

    }
}
