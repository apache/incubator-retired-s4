package org.apache.s4.core.ri;

import org.apache.s4.base.Event;
import org.apache.s4.base.KeyFinder;
import org.apache.s4.core.App;
import org.apache.s4.core.RemoteStream;
import org.apache.s4.wordcount.SentenceKeyFinder;
import org.apache.s4.wordcount.WordCountTest;

public class RemoteAdapterApp extends App {

    String outputStreamName;
    private RemoteStream remoteStream;

    @Override
    protected void onInit() {
        remoteStream = createOutputStream("inputStream", new SentenceKeyFinder());
    }

    protected KeyFinder<Event> remoteStreamKeyFinder;

    protected void setKeyFinder(KeyFinder<Event> keyFinder) {
        this.remoteStreamKeyFinder = keyFinder;
    }

    @Override
    protected void onStart() {
        injectSentence(WordCountTest.SENTENCE_1);
        injectSentence(WordCountTest.SENTENCE_2);
        injectSentence(WordCountTest.SENTENCE_3);

    }

    public void injectSentence(String sentence) {
        Event event = new Event();
        event.setStreamId("inputStream");
        event.put("sentence", String.class, sentence);
        getRemoteStream().put(event);
    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    public RemoteStream getRemoteStream() {
        return remoteStream;
    }
}
