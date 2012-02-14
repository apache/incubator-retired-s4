package org.apache.s4.edsl;

import java.util.Set;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Helper class to add a stream to an S4 application. This class and methods are private package. No need for app
 * developers to see this class.
 * 
 */
class StreamBuilder<T extends Event> {

    Class<T> type;
    String fieldName;
    Stream<T> stream;
    Set<String> pes = Sets.newHashSet();

    StreamBuilder(App app, Class<T> type) {

        Preconditions.checkNotNull(type);
        this.type = type;
        stream = app.createStream(type);
        stream.setName(type.getCanonicalName()); // Default name.
    }

    void setEventType(Class<T> type) {
        this.type = type;
    }

    /**
     * Name the stream.
     * 
     * @param name
     *            the stream name, default is an empty string.
     * @return the stream maker object
     */
    void setName(String name) {
        stream.setName(name);
    }

    /**
     * Define the key finder for this stream.
     * 
     * @param keyFinder
     *            a function to lookup the value of the key.
     */
    @SuppressWarnings("unchecked")
    void setKeyFinder(KeyFinder<?> keyFinder) {
        stream.setKey((KeyFinder<T>) keyFinder);
        stream.setName(type.getCanonicalName() + "," + keyFinder.getClass().getCanonicalName());
    }

    void setKey(String keyDescriptor) {

        stream.setKey(keyDescriptor);
        stream.setName(type.getCanonicalName() + "," + keyDescriptor);
    }

    // Not all PE may have been created, we use PE Name as a placeholder. The PE prototypes will be assigned in the
    // buildApp() method in AppBuilder.
    void to(String peName) {
        pes.add(peName);
    }

    void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
