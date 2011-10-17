package io.s4.wordcount;

import io.s4.base.Event;

public class StringEvent extends Event {
    
    String string;
    
    public StringEvent() {}
    
    public StringEvent(String string) {
        super();
        this.string = string;
    }
    
    public void setString(String string) {
        this.string = string;
    }
    
    public String getString() {
        return string;
    }

}
