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

package hello;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;

public class HelloPE extends ProcessingElement {

    // you should define downstream streams here and inject them in the app definition

    boolean seen = false;

    /**
     * This method is called upon a new Event on an incoming stream
     */
    public void onEvent(Event event) {
        // in this example, we use the default generic Event type, by you can also define your own type
        System.out.println("Hello " + (seen ? "again " : "") + event.get("name") + "!");
        seen = true;
    }

    @Override
    protected void onCreate() {
    }

    @Override
    protected void onRemove() {
    }

}
