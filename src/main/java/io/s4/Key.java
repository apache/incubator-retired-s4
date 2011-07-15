package io.s4;
import java.util.List;

import org.apache.commons.lang.StringUtils;


public class Key {

    final private Event event;
    final private KeyFinder finder;
    final private String separator;

    public Key(Event event, KeyFinder finder, String separator) {
        this.event = event;
        this.finder = finder;
        this.separator = separator;
    }
    
    public List<String> getList() {
        return finder.get(event);
    }
    
    public String get() {
        List<String> keys = getList();
        
        return StringUtils.join(keys, separator);
    }
}
