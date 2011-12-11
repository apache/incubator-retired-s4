package org.apache.s4.appbuilder;

import java.util.Collection;
import java.util.Map;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

abstract public class AppMaker {

    /**
     * NOTES: reflection+guice:
     * <code>http://groups.google.com/group/google-guice/browse_thread/thread/23f4bf986a999e00/73f83a98c288a3e1?lnk=gst&q=binding+api#73f83a98c288a3e1</code>
     */

    /**
     * The app graph is stored as follows:
     * <p>
     * PE to Stream
     * <p>
     * PE[1]: S[1,1], S[1,2], ...
     * <p>
     * PE[2]: S[2,1], S[2,2], ...
     * <p>
     * Stream to PE
     * <p>
     * S[1]: PE[1]
     * <p>
     * S[2] : PE[2]
     * 
     */

    private Multimap<PEMaker, StreamMaker> psGraph = LinkedListMultimap.create();
    private Map<StreamMaker, PEMaker> spGraph = Maps.newHashMap();

    public AppMaker() {

    }

    abstract protected void define();

    void add(PEMaker pem, StreamMaker stream) {

        psGraph.put(pem, stream);
    }

    void add(StreamMaker stream, PEMaker pem) {

        spGraph.put(stream, pem);
    }

    public PEMaker addPE(Class<? extends ProcessingElement> type) {
        return new PEMaker(this, type);
    }

    /**
     * Add a stream.
     * 
     * @param eventType
     *            the type of events emitted by this PE.
     * 
     * @return a stream maker.
     */
    public StreamMaker addStream(Class<? extends Event> type) {

        return new StreamMaker(this, type);

    }

    public App make() {
        return null;
    }

    public String toString() {

        StringBuilder sb = new StringBuilder();
        Map<PEMaker, Collection<StreamMaker>> psMap = psGraph.asMap();

        for (Map.Entry<PEMaker, Collection<StreamMaker>> entry : psMap.entrySet()) {
            sb.append(entry.getKey() + ": ");
            for (StreamMaker sm : entry.getValue()) {
                sb.append(sm + " ");
            }
            sb.append("\n");
        }

        for (Map.Entry<StreamMaker, PEMaker> entry : spGraph.entrySet()) {
            sb.append(entry.getKey() + ": " + entry.getValue());
            sb.append("\n");
        }

        return sb.toString();

    }
}
