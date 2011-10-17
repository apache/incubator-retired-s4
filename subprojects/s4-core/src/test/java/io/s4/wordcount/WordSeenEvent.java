package io.s4.wordcount;

import io.s4.base.Event;

public class WordSeenEvent extends Event {
    
    private String word;
    
    protected WordSeenEvent() {}

    public WordSeenEvent(String word) {
        super();
        this.word = word;
    }

    public String getWord() {
        return word;
    }


}
