package org.apache.s4.example.edsl.counter;

import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

public class TestCounterApp {

    @Test
    public void test() throws Exception {
        Injector injector = Guice.createInjector(new Module());
        CounterApp myApp = injector.getInstance(CounterApp.class);

        /* Normally. the container will handle this but this is just a test. */
        myApp.init();
        myApp.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        myApp.close();
    }
}
