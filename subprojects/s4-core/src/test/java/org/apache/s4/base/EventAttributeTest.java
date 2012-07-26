/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
