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
package io.s4.example;

import java.util.ArrayList;
import java.util.List;

import io.s4.App;
import io.s4.Event;
import io.s4.SingletonPE;
import io.s4.Stream;

public class GenerateUserEventPE extends SingletonPE {

    final private Stream<UserEvent>[] targetStreams;

    public GenerateUserEventPE(App app, Stream<UserEvent>... targetStreams) {
        super(app);
        this.targetStreams = targetStreams;
    }

    /* 
     * 
     */
    @Override
    protected void processInputEvent(Event event) {

    }

    @Override
    public void sendEvent() {
        List<String> favorites = new ArrayList<String>();
        favorites.add("dulce de leche");
        favorites.add("strawberry");
        UserEvent userEvent = new UserEvent("123abc", 25, favorites, 'f');
        
        for(int i=0; i<targetStreams.length; i++) {
            targetStreams[i].put(userEvent);
        }
    }
}
