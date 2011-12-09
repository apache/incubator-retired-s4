package org.apache.s4.base;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

public class EventAttributeTest {

    @Test
    public void test() {

        Map<String, String> map = new HashMap<String, String>();
        map.put("snoopy", "dog");

        SimpleEvent ev = new SimpleEvent("Hello");
        ev.put("An Int", Integer.class, 33);
        ev.put("A String", String.class, "XXX");
        ev.put("A Map", Map.class, map);

        Assert.assertEquals("XXX", ev.get("A String", String.class));
        Assert.assertEquals(33, ev.get("An Int", Integer.class).intValue());
        Assert.assertEquals("dog", ev.get("A Map", Map.class).get("snoopy"));
    }
}
