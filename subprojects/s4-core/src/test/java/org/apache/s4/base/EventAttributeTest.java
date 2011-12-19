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

        SimpleEvent ev = new SimpleEvent("Hello", 28);
        ev.put("An Int", Integer.class, 33);
        ev.put("A String", String.class, "XXX");
        ev.put("A Map", Map.class, map);

        Assert.assertEquals("XXX", ev.get("A String", String.class));
        Assert.assertEquals(33, ev.get("An Int", Integer.class).intValue());
        Assert.assertEquals("dog", ev.get("A Map", Map.class).get("snoopy"));
    }

    @Test
    public void testGenericKeyFinder1() {

        /* Try to get a field value and field exists. */

        KeyFinder<? extends Event> kf = new GenericKeyFinder<SimpleEvent>("numGrapes", SimpleEvent.class);

        SimpleEvent ev = new SimpleEvent("Hello", 28);
        @SuppressWarnings("unchecked")
        Key<SimpleEvent> k = new Key<SimpleEvent>((KeyFinder<SimpleEvent>) kf, "");
        Assert.assertEquals("28", k.get(ev));

    }

    @Test
    public void testGenericKeyFinder2() {

        /* Try to get an attribute value. */

        SimpleEvent ev = new SimpleEvent("Hello", 28);
        ev.put("An Int", Integer.class, 33);
        KeyFinder<? extends Event> kf = new GenericKeyFinder<SimpleEvent>("An Int", SimpleEvent.class);
        @SuppressWarnings("unchecked")
        Key<SimpleEvent> k = new Key<SimpleEvent>((KeyFinder<SimpleEvent>) kf, "");
        Assert.assertEquals("33", k.get(ev));
    }

    @Test(expected = NullPointerException.class)
    public void testGenericKeyFinder3() {

        /* Try to get a field that doesn't exist. */
        SimpleEvent ev = new SimpleEvent("Hello", 28);
        KeyFinder<? extends Event> kf = new GenericKeyFinder<SimpleEvent>("doesnotexist", SimpleEvent.class);
        @SuppressWarnings("unchecked")
        Key<SimpleEvent> k = new Key<SimpleEvent>((KeyFinder<SimpleEvent>) kf, "");
        Assert.assertNull(k.get(ev));
    }

}
