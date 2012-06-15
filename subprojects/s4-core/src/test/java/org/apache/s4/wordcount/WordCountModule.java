package org.apache.s4.wordcount;

import com.google.inject.AbstractModule;

public class WordCountModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(WordCountApp.class);

    }

}
