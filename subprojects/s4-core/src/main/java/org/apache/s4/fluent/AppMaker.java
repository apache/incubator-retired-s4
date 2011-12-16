package org.apache.s4.fluent;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.KeyFinder;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 * A fluent API to build S4 applications.
 * 
 * *
 * <p>
 * Usage example:
 * 
 * <pre>
 * 
 * public class MyApp extends AppMaker {
 * 
 *     &#064;Override
 *     protected void configure() {
 * 
 *         PEMaker pez, pey, pex;
 * 
 *         pez = addPE(PEZ.class);
 *         pez.addTrigger().fireOn(EventA.class).ifInterval(5, TimeUnit.SECONDS);
 *         pez.addCache().ofSize(1000).withDuration(3, TimeUnit.HOURS);
 * 
 *         pey = addPE(PEY.class).property(&quot;duration&quot;, 4).property(&quot;height&quot;, 99);
 *         pey.addTimer().withDuration(2, TimeUnit.MINUTES);
 * 
 *         pex = addPE(PEX.class).property(&quot;query&quot;, &quot;money&quot;);
 *         pex.addCache().ofSize(100).withDuration(1, TimeUnit.MINUTES);
 * 
 *         pey.emit(EventA.class).withKey(new DurationKeyFinder()).to(pez);
 *         pex.emit(EventB.class).withKey(new QueryKeyFinder()).to(pez);
 *         pex.emit(EventB.class).withKey(new QueryKeyFinder()).to(pey).to(pez);
 *     }
 * }
 * 
 * </pre>
 */
abstract public class AppMaker {

    private static final Logger logger = LoggerFactory.getLogger(AppMaker.class);

    /* Use multi-maps to save the graph. */
    private Multimap<PEMaker, StreamMaker> pe2stream = LinkedListMultimap.create();
    private Multimap<StreamMaker, PEMaker> stream2pe = LinkedListMultimap.create();

    final private App app;

    AppMaker() {
        this.app = new BaseApp();
    }

    /**
     * Configure the application.
     */
    abstract protected void configure();

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

    /**
     * Add a stream.
     * 
     * @param eventType
     *            the type of events emitted by this PE.
     * 
     * @return a stream maker.
     */
    protected StreamMaker addStream(Class<? extends Event> type) {
        StreamMaker stream = new StreamMaker(this, type);
        return stream;
    }

    App make() throws Exception {

        /* Loop PEMaker objects to create PEs. */
        for (PEMaker key : pe2stream.keySet()) {
            if (key != null) {
                key.setPe(makePE(key, key.getType()));
            }

        }
        /* Loop StreamMaker objects to create Streams. */
        for (StreamMaker key : stream2pe.keySet()) {
            if (key != null) {
                key.setStream(makeStream(key, key.getType()));
            }
        }

        /* PE to Stream wiring. */
        Set<PEMaker> done = Sets.newHashSet();
        Map<PEMaker, Collection<StreamMaker>> pe2streamMap = pe2stream.asMap();
        for (Map.Entry<PEMaker, Collection<StreamMaker>> entry : pe2streamMap.entrySet()) {
            PEMaker pm = entry.getKey();
            for (StreamMaker sm : entry.getValue()) {
                if (pm != null && sm != null && !done.contains(pm)) {
                    done.add(pm);
                    setStreamField(pm.getPe(), sm.getStream(), sm.getType());
                }
            }
        }

        /* Stream to PE wiring. */
        Map<StreamMaker, Collection<PEMaker>> stream2peMap = stream2pe.asMap();
        for (Map.Entry<StreamMaker, Collection<PEMaker>> entry : stream2peMap.entrySet()) {
            StreamMaker sm = entry.getKey();
            for (PEMaker pm : entry.getValue()) {
                if (pm != null && sm != null) {
                    sm.getStream().setPE(pm.getPe());
                }
            }
        }

        return app;
    }

    /* So the magic to create a Stream from a StreamMaker. */
    @SuppressWarnings("unchecked")
    private <T extends Event> Stream<T> makeStream(StreamMaker sm, Class<T> type) {

        Stream<T> stream = app.createStream(type);
        stream.setName(sm.getName());
        stream.setKey((KeyFinder<T>) sm.getKeyFinder()); // TODO: how do we make this safe?
        return stream;
    }

    /* Do the magic to create a PE from a PEMaker. */
    private <T extends ProcessingElement> T makePE(PEMaker pem, Class<T> type) throws NoSuchFieldException,
            IllegalAccessException {
        T pe = app.createPE(type);
        pe.setPECache(pem.getCacheMaximumSize(), pem.getCacheDuration(), TimeUnit.MILLISECONDS);
        pe.setTimerInterval(pem.getTimerInterval(), TimeUnit.MILLISECONDS);
        pe.setTrigger(pem.getTriggerEventType(), pem.getTriggerNumEvents(), pem.getTriggerInterval(),
                TimeUnit.MILLISECONDS);

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

    /* We need to set stream fields in PE classes. We will infer the field by checking the Event parameter type. */
    private <P extends ProcessingElement> void setStreamField(P pe, Stream<? extends Event> stream,
            Class<? extends Event> eventType) throws Exception {

        Field[] fields = pe.getClass().getDeclaredFields();
        String fieldName = "";
        Set<String> eventTypes = Sets.newHashSet();
        for (Field field : fields) {
            if (field.getType() == Stream.class) {

                fieldName = field.getName();
                if (field.getGenericType().toString().endsWith("<" + eventType.getCanonicalName() + ">")) {

                    /* Sanity check. This AOI does not support more than one stream field with the same event type. */
                    if (eventTypes.contains(field.getGenericType().toString())) {
                        logger.error(
                                "There is more than one stream field in PE [{}] for event type [{}]. The fluent API only supports one stream field per event type.",
                                pe.getClass().getName(), eventType.getCanonicalName());
                    }

                    eventTypes.add(field.getGenericType().toString());
                    logger.debug("Stream field [" + fieldName + "] in PE [" + pe.getClass().getCanonicalName()
                            + "] matches event type: [" + eventType.getCanonicalName() + "].");

                    /* Assign stream field. */
                    field.setAccessible(true);
                    field.set(pe, stream);
                }
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

    abstract protected void onStart();

    abstract protected void onInit();

    abstract protected void onClose();

}
