package org.apache.s4.fluent;

import java.lang.reflect.Field;

import org.junit.Test;

public class AppMakerTest {

    @Test
    public void test() throws Exception {

        MyApp myApp = new MyApp();
        myApp.setApp(new FluentApp(myApp));
        myApp.configure();
        System.out.println(myApp.toString());
        myApp.make();
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
