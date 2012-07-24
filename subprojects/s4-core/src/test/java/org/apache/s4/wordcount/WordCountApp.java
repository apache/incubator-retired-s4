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

package org.apache.s4.wordcount;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.Stream;

import com.google.inject.Inject;

public class WordCountApp extends App {

    protected boolean checkpointing = false;

    @Inject
    public WordCountApp() {
        super();
    }

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {

        WordClassifierPE wordClassifierPrototype = createPE(WordClassifierPE.class, "classifierPE");
        Stream<WordCountEvent> wordCountStream = createStream("words counts stream", new WordCountKeyFinder(),
                wordClassifierPrototype);
        WordCounterPE wordCounterPrototype = createPE(WordCounterPE.class, "counterPE");
        // wordCounterPrototype.setTrigger(WordSeenEvent.class, 1, 0, null);
        wordCounterPrototype.setWordClassifierStream(wordCountStream);
        Stream<WordSeenEvent> wordSeenStream = createStream("words seen stream", new WordSeenKeyFinder(),
                wordCounterPrototype);
        WordSplitterPE wordSplitterPrototype = createPE(WordSplitterPE.class);
        wordSplitterPrototype.setWordSeenStream(wordSeenStream);
        Stream<Event> sentenceStream = createInputStream("inputStream", new SentenceKeyFinder(), wordSplitterPrototype);
    }

    @Override
    protected void onClose() {

    }

}
