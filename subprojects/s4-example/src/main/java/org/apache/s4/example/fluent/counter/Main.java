/*
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *          http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the
 * License. See accompanying LICENSE file. 
 */
package org.apache.s4.example.fluent.counter;

import java.util.concurrent.TimeUnit;

import org.apache.s4.base.Event;
import org.apache.s4.core.Receiver;
import org.apache.s4.core.Sender;
import org.apache.s4.fluent.AppMaker;
import org.apache.s4.fluent.FluentApp;
import org.apache.s4.fluent.PEMaker;
import org.junit.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;

/*
 * This is a sample application to test a new S4 API. 
 * See README file for details.
 * 
 * */

final public class Main extends AppMaker {

    public Main() {
        super();
    }

    private PEMaker generateUserEventPE;

    /*
     * 
     * 
     * The application graph itself is created in this Class. However, developers may provide tools for creating apps
     * which will generate the objects.
     * 
     * IMPORTANT: we create a graph of PE prototypes. The prototype is a class instance that is used as a prototype from
     * which all PE instance will be created. The prototype itself is not used as an instance. (Except when the PE is of
     * type Singleton PE). To create a data structure for each PE instance you must do it in the method
     * ProcessingElement.onCreate().
     */

    /*
     * Build the application graph using POJOs. Don't like it? Write a nice tool.
     * 
     * @see io.s4.App#init()
     */
    @Override
    public void configure() {

        /* PE that prints counts to console. */
        PEMaker printPE = addPE(PrintPE.class).asSingleton();

        /* PEs that count events by user, gender, and age. */
        PEMaker userCountPE = addPE(CounterPE.class);
        userCountPE.addTrigger().fireOn(Event.class).ifInterval(100l, TimeUnit.MILLISECONDS);

        PEMaker genderCountPE = addPE(CounterPE.class);
        genderCountPE.addTrigger().fireOn(Event.class).ifInterval(100l, TimeUnit.MILLISECONDS);

        PEMaker ageCountPE = addPE(CounterPE.class);
        ageCountPE.addTrigger().fireOn(Event.class).ifInterval(100l, TimeUnit.MILLISECONDS);

        generateUserEventPE = addPE(GenerateUserEventPE.class).asSingleton();
        generateUserEventPE.addTimer().withDuration(1, TimeUnit.MILLISECONDS);

        ageCountPE.emit(CountEvent.class).onKey(new CountKeyFinder()).to(printPE);
        genderCountPE.emit(CountEvent.class).onKey(new CountKeyFinder()).to(printPE);
        userCountPE.emit(CountEvent.class).onKey(new CountKeyFinder()).to(printPE);

        generateUserEventPE.emit(UserEvent.class).onKey(new AgeKeyFinder()).to(ageCountPE);
        generateUserEventPE.emit(UserEvent.class).onKey(new GenderKeyFinder()).to(genderCountPE);
        generateUserEventPE.emit(UserEvent.class).onKey(new UserIDKeyFinder()).to(userCountPE);
    }

    /*
     * Create and send 200 dummy events of type UserEvent.
     * 
     * @see io.s4.App#start()
     */
    @Override
    public void start() {

    }

    @Override
    public void close() {
        System.out.println("Bye.");

    }

    // public static void main(String[] args) {
    @Test
    public void test() throws Exception {

        Injector injector = Guice.createInjector(new Module());
        FluentApp myApp = injector.getInstance(FluentApp.class);
        Sender sender = injector.getInstance(Sender.class);
        Receiver receiver = injector.getInstance(Receiver.class);
        myApp.setCommLayer(sender, receiver);

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
