/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.s4.edsl;

import java.util.Set;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Helper class to add a stream to an S4 application. This class and methods are private package. No need for app
 * developers to see this class.
 * 
 */
class StreamBuilder<T extends Event> {

    private static final Logger logger = LoggerFactory.getLogger(StreamBuilder.class);

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

    @SuppressWarnings("unchecked")
    void setKeyFinder(Class<?> type) {
        try {
            stream.setKey((KeyFinder<T>) type.newInstance());
        } catch (Exception e) {
            logger.error("Unable to create instance of KeyFinder [{}].", type.toString());
            e.printStackTrace();
        }
    }

    void setKey(String keyDescriptor) {

        stream.setKey(keyDescriptor);
        stream.setName(type.getCanonicalName() + "," + keyDescriptor);
    }

    // Not all PE may have been created, we use PE Name as a placeholder. The PE prototypes will be assigned in the
    // buildApp() method in AppBuilder.
    void to(String[] peNames) {
        for (int i = 0; i < peNames.length; i++) {
            logger.debug("to: " + peNames[i]);
            pes.add(peNames[i]);
        }
    }

    void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
}
