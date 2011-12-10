package org.apache.s4.appbuilder;

public class Main {

    /**
     * @param args
     */
    public static void main(String[] args) {

        AppMaker am = new AppMaker();

        PEMaker pem1, pem2;
        StreamMaker s1;
        StreamMaker s2, s3;

        pem1 = am.addPE(PEZ.class);

        s1 = am.addStream(EventA.class).withName("My first stream.").withKeyFinder("{gender}").to(pem1);

        pem2 = am.addPE(PEY.class).to(s1);

        s2 = am.addStream(EventB.class).withName("My second stream.").withKeyFinder("{age}").to(pem2);

        s3 = am.addStream(EventB.class).withName("My third stream.").withKeyFinder("{height}").to(pem2);

        am.addPE(PEX.class).to(s2).to(s3);
    }
}
