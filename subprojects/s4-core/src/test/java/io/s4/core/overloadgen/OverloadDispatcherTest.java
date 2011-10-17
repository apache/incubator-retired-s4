package io.s4.core.overloadgen;

import io.s4.core.gen.OverloadDispatcher;
import io.s4.core.gen.OverloadDispatcherGenerator;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Pattern;

import junit.framework.Assert;

import org.junit.Test;

public class OverloadDispatcherTest {

    @Test
    public void testDispatchWithEventHierarchies() throws Exception {
        OverloadDispatcherGenerator gen = new OverloadDispatcherGenerator(A.class);
        OverloadDispatcher dispatcher = (OverloadDispatcher) gen.generate().newInstance();
        A a = new A();
        // input events
        dispatcher.dispatchEvent(a, new Event1());
        Assert.assertEquals(Event1.class, a.processedEventClass);
        dispatcher.dispatchEvent(a, new Event1a());
        Assert.assertEquals(Event1a.class, a.processedEventClass);
        dispatcher.dispatchEvent(a, new Event2());
        Assert.assertEquals(Event2.class, a.processedEventClass);
       
        // trigger events
        dispatcher.dispatchTrigger(a, new Event2());
        Assert.assertEquals(Event2.class, a.processedTriggerEventClass);
        Assert.assertTrue(a.processedTriggerThroughGenericMethod);
        dispatcher.dispatchTrigger(a, new Event1());
        Assert.assertEquals(Event1.class, a.processedTriggerEventClass);
        Assert.assertFalse(a.processedTriggerThroughGenericMethod);
    }
    
    @Test
    public void testDispatchWithSingleMethod() throws Exception {
        OverloadDispatcherGenerator gen = new OverloadDispatcherGenerator(C.class);
        OverloadDispatcher dispatcher = (OverloadDispatcher) gen.generate().newInstance();
        C c = new C();
        dispatcher.dispatchEvent(c, new Event2());
        Assert.assertFalse(c.processedEvent1Class);
        dispatcher.dispatchEvent(c, new Event1());
        Assert.assertTrue(c.processedEvent1Class);
    }

    @Test
    public void testNoMatchingMethod() throws Exception {
        PrintStream stdout = System.out;
        try {
            ByteArrayOutputStream tmpOut = new ByteArrayOutputStream();
            System.setOut(new PrintStream(tmpOut));

            OverloadDispatcherGenerator gen = new OverloadDispatcherGenerator(B.class);
            OverloadDispatcher dispatcher = (OverloadDispatcher) gen.generate().newInstance();
            B b = new B();
            dispatcher.dispatchEvent(b, new Event1());
            String output = tmpOut.toString().trim();
            // use DOTALL to ignore previous lines in output debug mode
            Assert.assertTrue(Pattern.compile("^.+OverloadDispatcher\\d+ - Cannot dispatch event "
                    + "of type \\[" + Event1.class.getName() + "\\] to PE of type \\[" + B.class.getName()
                    + "\\] : no matching onEvent method found$", Pattern.DOTALL).matcher(output).matches());
        } finally {
            System.setOut(stdout);
        }

    }
    
    @Test
    public void testGenericEvent() throws Exception {
        OverloadDispatcherGenerator gen = new OverloadDispatcherGenerator(D.class);
        OverloadDispatcher dispatcher = (OverloadDispatcher) gen.generate().newInstance();
        D d = new D();
        dispatcher.dispatchEvent(d, new Event2());
        Assert.assertTrue(d.processedGenericEvent);
        dispatcher.dispatchEvent(d, new Event1());
        Assert.assertTrue(d.processedEvent1);
    }
}
