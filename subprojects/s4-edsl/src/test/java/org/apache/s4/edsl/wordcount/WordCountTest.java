package org.apache.s4.edsl.wordcount;

import org.junit.Test;

public class WordCountTest extends org.apache.s4.wordcount.WordCountTest {

    @Test
    public void testSimple() throws Exception {
        testWordCountApp(WordCountApp.class);

    }

}
