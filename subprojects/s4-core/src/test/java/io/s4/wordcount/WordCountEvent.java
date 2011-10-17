package io.s4.wordcount;

import io.s4.base.Event;

public class WordCountEvent extends Event {

        private String word;
        private int count;
        
        protected WordCountEvent() {}
        
        public WordCountEvent(String word, int count) {
            super();
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public int getCount() {
            return count;
        }

}
