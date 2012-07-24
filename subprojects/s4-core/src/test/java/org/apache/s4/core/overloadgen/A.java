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

package org.apache.s4.core.overloadgen;

import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;


public class A extends ProcessingElement {
    
    public Class<? extends Event> processedEventClass;
    public Class<? extends Event> processedTriggerEventClass;
    boolean processedTriggerThroughGenericMethod = false;
    
    public void onEvent(Event event) {
        processedEventClass = Event.class;
    }
        
    public void onEvent(Event2 event) {
        processedEventClass = event.getClass();
    }
    
    public void onEvent(Event1 event) {
        processedEventClass = event.getClass();
    }

    public void onEvent(Event1a event) {
        processedEventClass = event.getClass();
    }
    
    public void onTrigger(Event event) {
        processedTriggerEventClass = event.getClass();
        processedTriggerThroughGenericMethod = true;
    }
    
    public void onTrigger(Event1 event ) {
        processedTriggerEventClass = event.getClass();
        processedTriggerThroughGenericMethod = false;
    }
    
    
    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub
        
    }
    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub
        
    }

}
