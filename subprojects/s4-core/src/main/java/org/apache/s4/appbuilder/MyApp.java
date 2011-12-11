package org.apache.s4.appbuilder;

public class MyApp extends AppMaker {

    /**
     * @param args
     */
    public void define() {

        PEMaker pem1, pem2;
        StreamMaker s1;
        StreamMaker s2, s3;

        pem1 = addPE(PEZ.class);

        s1 = addStream(EventA.class).withName("My first stream.").withKey("{gender}").to(pem1);

        pem2 = addPE(PEY.class).to(s1);

        s2 = addStream(EventB.class).withName("My second stream.").withKey("{age}").to(pem2);

        s3 = addStream(EventB.class).withName("My third stream.").withKey("{height}").to(pem2);

        addPE(PEX.class).to(s2).to(s3);
    }

    public static void main(String[] args) {

        MyApp myApp = new MyApp();
        myApp.define();
        System.out.println(myApp.toString());
    }
}
