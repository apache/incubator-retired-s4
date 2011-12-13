package org.apache.s4.appmaker;

public class MyApp extends AppMaker {

    @Override
    protected void configure() {

        PEMaker pe1, pe2;
        StreamMaker s1;
        StreamMaker s2, s3;

        pe1 = addPE(PEZ.class);

        s1 = addStream(EventA.class).withName("My first stream.").withKey("{gender}").to(pe1);

        pe2 = addPE(PEY.class).to(s1);

        s2 = addStream(EventB.class).withName("My second stream.").withKey("{age}").to(pe2);

        s3 = addStream(EventB.class).withName("My third stream.").withKey("{height}").to(pe2);

        addPE(PEX.class).to(s2).to(s3);
    }
}
