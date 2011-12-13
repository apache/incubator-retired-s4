package org.apache.s4.appmaker;

import org.junit.Test;

public class AppMakerTest {

    @Test
    public void test() {

        MyApp myApp = new MyApp();
        myApp.configure();
        System.out.println(myApp.toString());
    }
}
