package org.apache.s4.appmaker;

public class MyApp extends AppMaker {

    @Override
    protected void configure() {

        PEMaker pez, pey;
        StreamMaker s1;
        StreamMaker s2, s3;

        pez = addPE(PEZ.class);

        s1 = addStream("stream1", EventA.class).withName("My first stream.").withKey("{gender}").to(pez);

        pey = addPE(PEY.class).to(s1).property("duration", 4).property("height", 99);

        s2 = addStream("stream2", EventB.class).withName("My second stream.").withKey("{age}").to(pey).to(pez);

        s3 = addStream("stream3", EventB.class).withKey("{height}").to(pey);

        addPE(PEX.class).to(s2).to(s3).property("keyword", "money");
    }

}
