package org.apache.s4.appbuilder;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

public class AppMaker {

    /**
     * The app graph is stored as follows:
     * <p>
     * 
     * <p>
     * PE[1]: S[1,1], S[1,2], ...
     * <p>
     * PE[2]: S[2,1], S[2,2], ...
     */

    private Multimap<PEMaker, StreamMaker> graph = LinkedListMultimap.create();

    public AppMaker() {

    }

    /**
     * Add a processing element.
     * 
     * @param streams
     *            events emitted by this PE will be put into these streams.
     * 
     * @return a pe maker.
     */
    // public <T extends Event> PEMaker addPE(StreamMaker<T>... streams) {
    //
    // PEMaker pem = new PEMaker();
    // for (int i = 0; i < streams.length; i++)
    // graph.put(pem, streams[i]);
    //
    // return pem;
    // }
    public PEMaker addPE(Class<? extends ProcessingElement> type) {
        return new PEMaker(type);
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

        return new StreamMaker(type);

    }

    public App make() {
        return null;
    }
}
