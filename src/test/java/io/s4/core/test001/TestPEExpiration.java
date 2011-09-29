package io.s4.core.test001;

import io.s4.example.counter.Module;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;

import ch.qos.logback.classic.Level;

import junit.framework.TestCase;

public class TestPEExpiration extends TestCase {

    public void testTimeInterval() {

        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.TRACE);

        Injector injector = Guice.createInjector(new Module());
        MyApp myApp = injector.getInstance(MyApp.class);
        myApp.init();
        myApp.start();

    }
}