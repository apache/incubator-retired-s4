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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * Implementation of the S4 embedded domain-specific language (EDSL).
 * 
 * <p>
 * To write an app extend this class and define the application graph using a chain of methods as follows:
 * 
 * <pre>
 *    final public class MyApp extends BuilderS4DSL {
 * 
 *     protected void onInit() {
 * 
 *         pe("Consumer").type(ConsumerPE.class).asSingleton().
 *         pe("Producer").type(ProducerPE.class).timer().withPeriod(1, TimeUnit.MILLISECONDS).asSingleton().
 *         emit(SomeEvent.class).withKey("someKey").to("Consumer").
 *         build()
 *    }
 * </pre>
 * 
 * <p>
 * A few things to notice:
 * <ul>
 * <li>Applications must extend class {@link BuilderS4DSL}
 * <li>The graph definition is implemented in the {@link App#onInit} method which is called by the container when the
 * application is loaded.
 * <li>PEs are defined using strings because they need to be referenced by other parts of the graph. By doing this, we
 * can create the whole application in a single chain of methods.
 * <li>To assign target streams to PE fields additional information may need to be provided using the {@code onField}
 * grammar token when there is an ambiguity. This will happen when a PE has more than one targetStream field with the
 * same {@link Event} type. Use the construct {@code emit(SomeEvent.class).onField("streamFieldName")}. If the PE
 * doesn't have a field named {@code "streamField"} whose stream parameter type is {@code someEvent)} then the parser
 * will fail to build the app.
 * <li>To configure a PE, set property values by chaining any number of {@code prop(name, value)} methods. The name
 * should match a PE field and the value will be parsed using the type of that field.
 * </ul>
 * <p>
 * Grammar:
 * 
 * <pre>
 *  (pe , type , prop* , (fireOn , afterInterval? , afterNumEvents?)? , (timer, withPeriod)? ,
 *  (cache, size , expires? )? , asSingleton? , (emit, onField?,
 *  (withKey|withKeyFinder)?, to )*  )+ , build
 * </pre>
 * 
 * <p>
 * See the <a href="http://code.google.com/p/diezel">Diezel</a> project for details.
 * 
 */
public class AppBuilder extends App {

    protected App app = this;

    static final Logger logger = LoggerFactory.getLogger(AppBuilder.class);

    private Multimap<ProcessingElement, StreamBuilder<? extends Event>> pe2stream = LinkedListMultimap.create();
    Set<StreamBuilder<? extends Event>> streamBuilders = Sets.newHashSet();

    /* Variables used to hold values from state to state. */
    ProcessingElement processingElement;
    String peName;
    Class<? extends Event> triggerEventType;
    long triggerInterval = 0;
    TimeUnit triggerTimeUnit;
    int cacheSize;
    StreamBuilder<? extends Event> streamBuilder;
    String propertyName, propertyValue;

    public static AppBuilder getAppBuilder() {
        return new BuilderS4DSL();
    }

    void addProperty(String name, String value) {
        propertyName = name;
        propertyValue = value;
        setField();
    }

    void addPe2Stream(ProcessingElement pe, StreamBuilder<? extends Event> st) {
        pe2stream.put(pe, st);
    }

    App buildApp() {

        /* Stream to PE writing. */
        for (StreamBuilder<? extends Event> sb : streamBuilders) {
            for (String peName : sb.pes) {
                ProcessingElement pe = getPE(peName);
                sb.stream.setPEs(pe);
            }
        }

        /* PE to Stream wiring. */
        Map<ProcessingElement, Collection<StreamBuilder<? extends Event>>> pe2streamMap = pe2stream.asMap();
        for (Map.Entry<ProcessingElement, Collection<StreamBuilder<? extends Event>>> entry : pe2streamMap.entrySet()) {
            ProcessingElement pe = entry.getKey();
            Collection<StreamBuilder<? extends Event>> streams = entry.getValue();

            if (pe != null && streams != null) {
                try {
                    setStreamField(pe, streams);
                } catch (Exception e) {
                    logger.error("Unable to build app.", e);
                    return null;
                }
            }
        }

        return this;
    }

    /**
     * @param peName
     *            the peName to set
     */
    protected void setPeName(String peName) {
        this.peName = peName;
    }

    /*
     * Cannot create an abstract class in Diezel so for now, I just implement the abstract methods here. They need to be
     * overloaded by the app developer.
     */
    @Override
    protected void onStart() {
    }

    @Override
    protected void onInit() {
    }

    @Override
    protected void onClose() {
    }

    private <T extends ProcessingElement> void setField() {

        logger.debug("Adding property [{}] to PE of type [{}].", propertyName, processingElement.getClass().getName());

        Class<? extends ProcessingElement> type = processingElement.getClass();

        try {
            Field f = type.getDeclaredField(propertyName);
            f.setAccessible(true);
            logger.trace("Type: {}.", f.getType());
            logger.trace("GenericType: {}.", f.getGenericType());

            /* Set the field. */
            if (f.getType().getCanonicalName() == "long") {
                f.setLong(processingElement, Long.parseLong(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "int") {
                f.setInt(processingElement, Integer.parseInt(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "float") {
                f.setFloat(processingElement, Float.parseFloat(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "double") {
                f.setDouble(processingElement, Double.parseDouble(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "short") {
                f.setShort(processingElement, Short.parseShort(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "byte") {
                f.setByte(processingElement, Byte.parseByte(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "boolean") {
                f.setBoolean(processingElement, Boolean.parseBoolean(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "char") {
                f.setChar(processingElement, (char) Byte.parseByte(propertyValue));
                return;
            } else if (f.getType().getCanonicalName() == "java.lang.String") {
                f.set(processingElement, propertyValue);
                return;
            }

            logger.error("Unable to set field named [{}] in PE of type [{}].", propertyName, type);
            throw new IllegalArgumentException();

            // production code should handle these exceptions more gracefully
        } catch (NoSuchFieldException e) {
            logger.error("There is no field named [{}] in PE of type [{}].", propertyName, type);
        } catch (Exception e) {
            logger.error("Couldn't set value for field [{}] in PE of type [{}].", propertyName, type);
        }
    }

    /* Set the stream fields in PE classes. Infer the field by checking the stream parameter type <? extends Event>. */
    private void setStreamField(ProcessingElement pe, Collection<StreamBuilder<? extends Event>> streams)
            throws Exception {

        /*
         * Create a map of the stream fields to the corresponding generic type. We will use this info to assign the
         * streams. If the field type matches the stream type and there is no ambiguity, then the assignment is easy. If
         * more than one field has the same type, then then we need to do more work.
         */
        Field[] fields = pe.getClass().getDeclaredFields();
        Multimap<String, Field> typeMap = LinkedListMultimap.create();
        logger.debug("Analyzing PE [{}].", pe.getClass().getName());
        for (Field field : fields) {
            logger.trace("Field [{}] is of generic type [{}].", field.getName(), field.getGenericType());

            if (field.getType() == Stream[].class) {
                logger.debug("Found stream field: {}", field.getGenericType());

                /* Track what fields have streams with the same event type. */
                String key = field.getGenericType().toString();
                typeMap.put(key, field);
            }
        }

        /* Assign streams to stream fields. */
        Multimap<Field, Stream<? extends Event>> assignment = LinkedListMultimap.create();
        for (StreamBuilder<? extends Event> sm : streams) {

            Stream<? extends Event> stream = sm.stream;
            Class<? extends Event> eventType = sm.type;
            String key = Stream.class.getCanonicalName() + "<" + eventType.getCanonicalName() + ">[]";
            if (typeMap.containsKey(key)) {
                String fieldName;
                Field field;
                Collection<Field> streamFields = typeMap.get(key);
                int numStreamFields = streamFields.size();
                logger.debug("Found [{}] stream fields for type [{}].", numStreamFields, key);

                if (numStreamFields > 1) {

                    /*
                     * There is more than one field that can be used for this stream type. To resolve the ambiguity we
                     * need additional information. The app graph should include the name of the field that should be
                     * used to assign this stream. If the name is missing we bail out.
                     */
                    fieldName = sm.fieldName;

                    /* Bail out. */
                    if (fieldName == null) {
                        String msg = String
                                .format("There are [%d] stream fields in PE [%s]. To assign stream [%s] you need to provide the field name in the application graph using the method withFiled(). See Javadocs for an example.",
                                        numStreamFields, pe.getClass().getName(), stream.getName());
                        logger.error(msg);
                        throw new Exception(msg);
                    }

                    /* Use the provided field name to choose the PE field. */
                    field = pe.getClass().getDeclaredField(fieldName);

                } else {

                    /*
                     * The easy case, no ambiguity, we don't need an explicit field name to be provided. We have the
                     * field that matches the stream type.
                     */
                    Iterator<Field> iter = streamFields.iterator();
                    field = iter.next(); // Note that numStreamFields == 1, the size of this collection is 1.
                    logger.debug("Using field [{}].", field.getName());
                }

                /*
                 * By now, we found the field to use for this stream or we bailed out. We are not ready to finish yet.
                 * There may be more than one stream that needs to be assigned to this field. The stream fields must be
                 * arrays by convention and there may be more than one stream assigned to this fields. For now we create
                 * a multimap from field to streams so we can construct the array in the next step.
                 */
                assignment.put(field, stream);

            } else {

                /* We couldn't find a match. Tell user to fix the EDSL code. */
                String msg = String.format(
                        "There is no stream of type [%s] in PE [%s]. I was unable to assign stream [%s].", key, pe
                                .getClass().getName(), stream.getName());
                logger.error(msg);
                throw new Exception(msg);

            }
        }
        /* Now we construct the array and do the final assignment. */

        Map<Field, Collection<Stream<? extends Event>>> assignmentMap = assignment.asMap();
        for (Map.Entry<Field, Collection<Stream<? extends Event>>> entry : assignmentMap.entrySet()) {
            Field f = entry.getKey();

            int arraySize = entry.getValue().size();
            @SuppressWarnings("unchecked")
            Stream<? extends Event> streamArray[] = (Stream<? extends Event>[]) Array.newInstance(Stream.class,
                    arraySize);
            int i = 0;
            for (Stream<? extends Event> s : entry.getValue()) {
                streamArray[i++] = s;

                f.setAccessible(true);
                f.set(pe, streamArray);
                logger.debug("Assigned [{}] streams to field [{}].", streamArray.length, f.getName());
            }
        }
    }

    void clearPEState() {
        propertyName = null;
        propertyValue = null;
        processingElement = null;
        peName = null;
        triggerEventType = null;
        triggerTimeUnit = null;
        cacheSize = -1;
        streamBuilder = null;
    }

}
