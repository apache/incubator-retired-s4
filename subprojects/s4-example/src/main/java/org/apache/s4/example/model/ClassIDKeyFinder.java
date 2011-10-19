/*
 * Copyright (c) 2011 The S4 Project, http://s4.io.
 * All rights reserved.
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
package org.apache.s4.example.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.s4.core.KeyFinder;


public class ClassIDKeyFinder implements KeyFinder<ObsEvent> {

    @Override
    public List<String> get(ObsEvent event) {

        List<String> results = new ArrayList<String>();

        /* Retrieve the user ID and add it to the list. */
        results.add(Integer.toString(event.getClassId()));

        return results;
    }

}
