package org.apache.s4.appmaker;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.fluent.FluentApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.AbstractModule;

/**
 * A fluent API to build S4 applications.
 * 
 * *
 * <p>
 * Usage example:
 * 
 * <pre>
 * public class MyApp extends AppMaker {
 * 
 *     &#064;Override
 *     void configure() {
 * 
 *         PEMaker pe1, pe2;
 *         StreamMaker s1;
 *         StreamMaker s2, s3;
 * 
 *         pe1 = addPE(PEZ.class);
 * 
 *         s1 = addStream(EventA.class).withName(&quot;My first stream.&quot;).withKey(&quot;{gender}&quot;).to(pe1);
 * 
 *         pe2 = addPE(PEY.class).to(s1);
 * 
 *         s2 = addStream(EventB.class).withName(&quot;My second stream.&quot;).withKey(&quot;{age}&quot;).to(pe2);
 * 
 *         s3 = addStream(EventB.class).withName(&quot;My third stream.&quot;).withKey(&quot;{height}&quot;).to(pe2);
 * 
 *         addPE(PEX.class).to(s2).to(s3);
 *     }
 * }
 * </pre>
 */
abstract public class AppMaker {

    private static final Logger logger = LoggerFactory.getLogger(AppMaker.class);

    /* Use multi-maps to save the graph. */
    private Multimap<PEMaker, StreamMaker> pe2stream = LinkedListMultimap.create();
    private Multimap<StreamMaker, PEMaker> stream2pe = LinkedListMultimap.create();
    private List<Element> order = Lists.newLinkedList();

    /**
     * Configure the application.
     */
    abstract protected void configure();

    /* Used internally to build the graph. */
    void add(PEMaker pem, StreamMaker stream) {

        pe2stream.put(pem, stream);
        logger.trace("Adding pe [{}] to stream [{}].", pem, stream);
    }

    /* Used internally to build the graph. */
    void add(StreamMaker stream, PEMaker pem) {

        stream2pe.put(stream, pem);
        logger.trace("Adding stream [{}] to pe [{}].", stream, pem);
    }

    protected PEMaker addPE(Class<? extends ProcessingElement> type) {
        PEMaker pe = new PEMaker(this, type);
        order.add(new Element(pe, null));
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
    protected StreamMaker addStream(String propName, Class<? extends Event> type) {
        StreamMaker stream = new StreamMaker(this, propName, type);
        order.add(new Element(null, stream));
        return stream;
    }

    App make() {

        App app = null;

        /* Build the graph using the same order as configured in AppMaker. */
        for (Element element : order) {

            if (element.pe != null) {
                /* Create a PE. */
                ProcessingElement pe = app.createPE(element.pe.getType());

            } else {
                /* Create a stream. */

            }
        }

        Map<PEMaker, Collection<StreamMaker>> pe2streamMap = pe2stream.asMap();
        for (Map.Entry<PEMaker, Collection<StreamMaker>> entry : pe2streamMap.entrySet()) {
            // sb.append(entry.getKey() + ": ");
            for (StreamMaker sm : entry.getValue()) {
                // sb.append(sm + " ");
            }
        }

        Map<StreamMaker, Collection<PEMaker>> stream2peMap = stream2pe.asMap();
        for (Map.Entry<StreamMaker, Collection<PEMaker>> entry : stream2peMap.entrySet()) {
            // sb.append(entry.getKey() + ": ");
            for (PEMaker pm : entry.getValue()) {
                // sb.append(pm + " ");
            }
        }

        return null;
    }

    /**
     * A printable representation of the application graph.
     * 
     * @return the application graph.
     */
    public String toString() {

        StringBuilder sb = new StringBuilder();

        Map<PEMaker, Collection<StreamMaker>> pe2streamMap = pe2stream.asMap();
        for (Map.Entry<PEMaker, Collection<StreamMaker>> entry : pe2streamMap.entrySet()) {
            sb.append(entry.getKey() + ": ");
            for (StreamMaker sm : entry.getValue()) {
                sb.append(sm + " ");
            }
            sb.append("\n");
        }

        Map<StreamMaker, Collection<PEMaker>> stream2peMap = stream2pe.asMap();
        for (Map.Entry<StreamMaker, Collection<PEMaker>> entry : stream2peMap.entrySet()) {
            sb.append(entry.getKey() + ": ");
            for (PEMaker pm : entry.getValue()) {
                sb.append(pm + " ");
            }
            sb.append("\n");
        }

        return sb.toString();

    }

    class Element {

        PEMaker pe;
        StreamMaker stream;

        Element(PEMaker pe, StreamMaker stream) {
            this.pe = pe;
            this.stream = stream;
        }

    }

    class Module extends AbstractModule {

        @Override
        protected void configure() {

            bind(FluentApp.class);
            bind(PEX.class);
        }
    }
}
