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

package org.apache.s4.base;

import java.lang.reflect.Field;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 
 * Implementation of {@link KeyFinder} that uses introspection on the target Event to create the key finder.
 * <p>
 * The search for the key is as follows:
 * <p>
 * <ul>
 * <li>If the event object class extends {@link Event}, find a field that matches the key name.
 * <li>If there is no match and the {@link Event} class has arbitrary attributes, search for the key.
 * <li>Otherwise, fail.
 * </ul>
 * 
 * 
 * @param <T>
 */
public class GenericKeyFinder<T extends Event> implements KeyFinder<T> {

    private static final Logger logger = LoggerFactory.getLogger(GenericKeyFinder.class);

    final private String keyName;
    private final Class<T> eventType;
    private final Field field;

    public GenericKeyFinder(String keyName, Class<T> eventType) throws SecurityException {
        this.keyName = keyName;
        this.eventType = eventType;

        logger.debug("Creating a generic key finder for key [{}] with event type [{}].", keyName, eventType.getName());
        field = getField();
    }

    @Override
    public List<String> get(T event) {

        List<String> list = Lists.newArrayList();

        if (field != null) {

            try {
                list.add(String.valueOf(field.get(event)));
            } catch (IllegalArgumentException e) {
                logger.error("Unable to access field [{}] in event of type [{}].", field.getName(), eventType.getName());
                throw e;
            } catch (IllegalAccessException e) {
                logger.error("Could not access field.", e);
                return null;
            }

        } else {
            list.add(event.get(keyName));
        }
        return list;
    }

    private Field getField() throws SecurityException {

        Field f;

        /* Find a field with name keyName. */
        try {
            f = eventType.getDeclaredField(keyName);
            logger.debug("Found field [{}] of type [{}].", f.getName(), f.getType());

            f.setAccessible(true);
            return f;
        } catch (NoSuchFieldException e) {

            logger.debug(
                    "Field [{}] could not be found in class [{}]. I will check if this is an Event attribute at run-time.",
                    keyName, eventType.getName());
        }
        return null;
    }
}
