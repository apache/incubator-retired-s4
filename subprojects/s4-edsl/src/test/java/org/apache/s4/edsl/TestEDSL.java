package org.apache.s4.edsl;

import java.lang.reflect.Field;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestEDSL {

    @Test
    public void test() throws Exception {
        Injector injector = Guice.createInjector(new Module());
        MyApp myApp = injector.getInstance(MyApp.class);

        /* Normally. the container will handle this but this is just a test. */
        myApp.init();
        myApp.start();
        myApp.close();
    }

    @Test
    public void testReflection() {

        try {
            Class<?> c = PEY.class;
            Field f = c.getDeclaredField("duration");
            System.out.format("Type: %s%n", f.getType());
            System.out.format("GenericType: %s%n", f.getGenericType());

            // production code should handle these exceptions more gracefully
        } catch (NoSuchFieldException x) {
            x.printStackTrace();
        }
    }

}
