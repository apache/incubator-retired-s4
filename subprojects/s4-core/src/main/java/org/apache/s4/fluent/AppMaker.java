package org.apache.s4.fluent;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

/**
 * A fluent API to build S4 applications.
 * 
 * *
 * <p>
 * Usage example:
 * 
 * <pre>
 * 
 * &#064;Override
 * public void configure() {
 * 
 *     PEMaker pez, pey, pex;
 * 
 *     pez = addPE(PEZ.class);
 *     pez.addTrigger().fireOn(EventA.class).ifInterval(5, TimeUnit.SECONDS);
 *     pez.addCache().ofSize(1000).withDuration(3, TimeUnit.HOURS);
 * 
 *     pey = addPE(PEY.class).with(&quot;duration&quot;, 4).with(&quot;height&quot;, 99);
 *     pey.addTimer().withDuration(2, TimeUnit.MINUTES);
 * 
 *     pex = addPE(PEX.class).with(&quot;query&quot;, &quot;money&quot;).asSingleton();
 *     pex.addCache().ofSize(100).withDuration(1, TimeUnit.MINUTES);
 * 
 *     pey.emit(EventA.class).withField(&quot;stream3&quot;).onKey(new DurationKeyFinder()).to(pez);
 *     pey.emit(EventA.class).withField(&quot;heightpez&quot;).onKey(new HeightKeyFinder()).to(pez);
 *     pez.emit(EventB.class).to(pex);
 *     pex.emit(EventB.class).onKey(new QueryKeyFinder()).to(pey).to(pez);
 * }
 * 
 * 
 * </pre>
 */
abstract public class AppMaker {

    private static final Logger logger = LoggerFactory.getLogger(AppMaker.class);

    /* Use multi-maps to save the graph. */
    private Multimap<PEMaker, StreamMaker> pe2stream = LinkedListMultimap.create();
    private Multimap<StreamMaker, PEMaker> stream2pe = LinkedListMultimap.create();

    private FluentApp app;

    public AppMaker() {

    }

    public void setApp(FluentApp app) {
        this.app = app;
    }

    /**
     * Configure the application.
     */
    protected abstract void start();

    protected abstract void configure();

    protected abstract void close();

    /**
     * @return the app
     */
    public FluentApp getApp() {
        return app;
    }

    /* Used internally to build the graph. */
    void add(PEMaker pem, StreamMaker stream) {

        pe2stream.put(pem, stream);
        logger.debug("Adding pe [{}] to stream [{}].", pem != null ? pem.getType().getName() : "null",
                stream != null ? stream.getName() : "null");
    }

    /* Used internally to build the graph. */
    void add(StreamMaker stream, PEMaker pem) {

        stream2pe.put(stream, pem);
        logger.debug("Adding stream [{}] to pe [{}].", stream != null ? stream.getName() : "null", pem != null ? pem
                .getType().getName() : "null");
    }

    protected PEMaker addPE(Class<? extends ProcessingElement> type) {
        PEMaker pe = new PEMaker(this, type);
        return pe;
    }

    App make() {

        logger.debug("Start MAKE.");

        /* Loop PEMaker objects to create PEs. */
        for (PEMaker key : pe2stream.keySet()) {
            if (key != null) {
                try {
                    key.setPe(makePE(key, key.getType()));
                } catch (NoSuchFieldException e) {
                    logger.error("Couldn't make PE.", e);
                } catch (IllegalAccessException e) {
                    logger.error("Couldn't make PE.", e);
                }
            }

        }
        /* Loop StreamMaker objects to create Streams. */
        for (StreamMaker key : stream2pe.keySet()) {
            if (key != null) {
                key.setStream(makeStream(key, key.getType()));
            }
        }

        /* PE to Stream wiring. */
        Map<PEMaker, Collection<StreamMaker>> pe2streamMap = pe2stream.asMap();
        for (Map.Entry<PEMaker, Collection<StreamMaker>> entry : pe2streamMap.entrySet()) {
            PEMaker pm = entry.getKey();
            Collection<StreamMaker> streams = entry.getValue();

            if (pm != null && streams != null) {
                try {
                    setStreamField(pm, streams);
                } catch (Exception e) {
                    logger.error("Couldn't make Stream.", e);
                }
            }
        }

        /* Stream to PE wiring. */
        Map<StreamMaker, Collection<PEMaker>> stream2peMap = stream2pe.asMap();
        for (Map.Entry<StreamMaker, Collection<PEMaker>> entry : stream2peMap.entrySet()) {
            StreamMaker sm = entry.getKey();
            for (PEMaker pm : entry.getValue()) {
                if (pm != null && sm != null) {
                    // sm.getStream().setPE(pm.getPe());
                }
            }
        }

        return app;
    }

    /* Do the magic to create a Stream from a StreamMaker. */
    @SuppressWarnings("unchecked")
    private <T extends Event> Stream<T> makeStream(StreamMaker sm, Class<T> type) {

        // Stream<T> stream = app.createStream(type);
        // stream.setName(sm.getName());
        //
        // if (sm.getKeyFinder() != null)
        // stream.setKey((KeyFinder<T>) sm.getKeyFinder());
        // else if (sm.getKeyDescriptor() != null)
        // stream.setKey(sm.getKeyDescriptor());
        //
        // return stream;
        return null;
    }

    /* Do the magic to create a PE from a PEMaker. */
    private <T extends ProcessingElement> T makePE(PEMaker pem, Class<T> type) throws NoSuchFieldException,
            IllegalAccessException {
        T pe = app.createPE(type);
        pe.setSingleton(pem.isSingleton());

        if (pem.getCacheMaximumSize() > 0)
            pe.setPECache(pem.getCacheMaximumSize(), pem.getCacheDuration(), TimeUnit.MILLISECONDS);

        if (pem.getTimerInterval() > 0)
            pe.setTimerInterval(pem.getTimerInterval(), TimeUnit.MILLISECONDS);

        if (pem.getTriggerEventType() != null) {
            if (pem.getTriggerNumEvents() > 0 || pem.getTriggerInterval() > 0) {
                pe.setTrigger(pem.getTriggerEventType(), pem.getTriggerNumEvents(), pem.getTriggerInterval(),
                        TimeUnit.MILLISECONDS);
            }
        }

        /* Use introspection to match properties to class fields. */
        setPEAttributes(pe, pem, type);
        return pe;
    }

    private <T extends ProcessingElement> void setPEAttributes(T pe, PEMaker pem, Class<T> type)
            throws NoSuchFieldException, IllegalAccessException {

        PropertiesConfiguration properties = pem.getProperties();
        @SuppressWarnings("unchecked")
        Iterator<String> iter = properties.getKeys();

        while (iter.hasNext()) {
            String property = iter.next();
            logger.debug("Adding property [{}] to PE of type [{}].", property, type.getName());
            setField(property, pe, pem, type);
        }
    }

    private <T extends ProcessingElement> void setField(String fieldName, T pe, PEMaker pm, Class<T> type)
            throws NoSuchFieldException, IllegalAccessException {
        try {
            Field f = type.getDeclaredField(fieldName);
            f.setAccessible(true);
            logger.trace("Type: {}.", f.getType());
            logger.trace("GenericType: {}.", f.getGenericType());

            /* Set the field. */
            if (f.getType().getCanonicalName() == "long") {
                f.setLong(pe, pm.getProperties().getLong(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "int") {
                f.setInt(pe, pm.getProperties().getInt(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "float") {
                f.setFloat(pe, pm.getProperties().getFloat(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "double") {
                f.setDouble(pe, pm.getProperties().getDouble(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "short") {
                f.setShort(pe, pm.getProperties().getShort(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "byte") {
                f.setByte(pe, pm.getProperties().getByte(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "boolean") {
                f.setBoolean(pe, pm.getProperties().getBoolean(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "char") {
                f.setChar(pe, (char) pm.getProperties().getByte(fieldName));
                return;
            } else if (f.getType().getCanonicalName() == "java.lang.String") {
                f.set(pe, pm.getProperties().getString(fieldName));
                return;
            }

            logger.error("Unable to set field named [{}] in PE of type [{}].", fieldName, type);
            throw new IllegalArgumentException();

            // production code should handle these exceptions more gracefully
        } catch (NoSuchFieldException e) {
            logger.error("There is no field named [{}] in PE of type [{}].", fieldName, type);
            throw e;
        } catch (IllegalArgumentException e) {
            logger.error("Couldn't set value for field [{}] in PE of type [{}].", fieldName, type);
            throw e;
        }
    }

    /* Set the stream fields in PE classes. Infer the field by checking the stream parameter type <? extends Event>. */
    private void setStreamField(PEMaker pm, Collection<StreamMaker> streams) throws Exception {

        /*
         * Create a map of the stream fields to the corresponding generic type. We will use this info to assign the
         * streams. If the field type matches the stream type and there is no ambiguity, then the assignment is easy. If
         * more than one field has the same type, then then we need to do more work.
         */
        Field[] fields = pm.getPe().getClass().getDeclaredFields();
        Multimap<String, Field> typeMap = LinkedListMultimap.create();
        logger.debug("Analyzing PE [{}].", pm.getPe().getClass().getName());
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
        for (StreamMaker sm : streams) {

            if (sm == null)
                continue;

            Stream<? extends Event> stream = sm.getStream();
            Class<? extends Event> eventType = sm.getType();
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
                    fieldName = sm.getFieldName();

                    /* Bail out. */
                    if (fieldName == null) {
                        String msg = String
                                .format("There are [%d] stream fields in PE [%s]. To assign stream [%s] you need to provide the field name in the application graph using the method withFiled(). See Javadocs for an example.",
                                        numStreamFields, pm.getPe().getClass().getName(), stream.getName());
                        logger.error(msg);
                        throw new Exception(msg);
                    }

                    /* Use the provided field name to choose the PE field. */
                    field = pm.getPe().getClass().getDeclaredField(fieldName);

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
                 * a multimap from field to streams so we can construct the array in the next pass.
                 */
                assignment.put(field, stream);

            } else {

                /* We couldn't find a match. Tell user to fix the application. */
                String msg = String.format(
                        "There is no stream of type [%s] in PE [%s]. I was unable to assign stream [%s].", key, pm
                                .getPe().getClass().getName(), stream.getName());
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
                f.set(pm.getPe(), streamArray);
                logger.debug("Assigned [{}] streams to field [{}].", streamArray.length, f.getName());
            }
        }
    }

    static private String toString(PEMaker pm) {
        return pm != null ? pm.getType().getName() + " " : "null ";
    }

    static private String toString(StreamMaker sm) {
        return sm != null ? sm.getName() + " " : "null ";
    }

    /**
     * A printable representation of the application graph.
     * 
     * @return the application graph.
     */
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("\nApplication Graph for " + this.getClass().getCanonicalName() + "\n");
        Map<PEMaker, Collection<StreamMaker>> pe2streamMap = pe2stream.asMap();
        for (Map.Entry<PEMaker, Collection<StreamMaker>> entry : pe2streamMap.entrySet()) {
            sb.append(toString(entry.getKey()) + "=> ");
            for (StreamMaker sm : entry.getValue()) {
                sb.append(toString(sm));
            }
            sb.append("\n");
        }

        Map<StreamMaker, Collection<PEMaker>> stream2peMap = stream2pe.asMap();
        for (Map.Entry<StreamMaker, Collection<PEMaker>> entry : stream2peMap.entrySet()) {
            sb.append(toString(entry.getKey()) + "=> ");
            for (PEMaker pm : entry.getValue()) {
                sb.append(toString(pm));
            }
            sb.append("\n");
        }

        return sb.toString();

    }

}
