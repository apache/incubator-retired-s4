package org.apache.s4.edsl;

import java.lang.reflect.Field;

import org.apache.s4.comm.DefaultCommModule;
import org.apache.s4.core.DefaultCoreModule;
import org.apache.s4.fixtures.ZkBasedTest;
import org.junit.Test;

import com.google.common.io.Resources;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestEDSL extends ZkBasedTest {

    public final static String CLUSTER_NAME = "cluster1";

    @Test
    public void test() throws Exception {
        Injector injector = Guice.createInjector(
                new DefaultCommModule(Resources.getResource("default.s4.comm.properties").openStream(), CLUSTER_NAME),
                new DefaultCoreModule(Resources.getResource("default.s4.core.properties").openStream()));
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
