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
package io.s4.example.counter;

import io.s4.Event;

import java.util.List;

public class UserEvent extends Event {
        
    final private String userID;
    final private int age;
    final private char gender;  
    final private List<String> favorites;
    
    UserEvent(String userID, int age, List<String> favorites, char gender) {
        this.userID = userID;
        this.age = age;
        this.favorites = favorites;
        this.gender = gender;
    }

    /**
     * @return the userID
     */
    public String getUserID() {
        return userID;
    }
    
    /**
     * @return the age
     */
    public int getAge() {
        return age;
    }
    
    /**
     * @return the favorites
     */
    public List<String> getFavorites() {
        return favorites;
    }
    
    /**
     * @return the gender
     */
    public char getGender() {
        return gender;
    }
}
