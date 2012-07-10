package s4app;

import java.util.concurrent.TimeUnit;

import org.apache.s4.core.App;

public class ProducerApp extends App {

    private ProducerPE producerPE;

    @Override
    protected void onStart() {
        System.out.println("Starting CounterApp...");
        producerPE.getInstanceForKey("single");
    }

    // generic array due to varargs generates a warning.
    @Override
    protected void onInit() {
        System.out.println("Initing CounterApp...");

        producerPE = createPE(ProducerPE.class, "producer");
        producerPE.setStreams(createOutputStream("tickStream"));
        producerPE.setTimerInterval(10, TimeUnit.MILLISECONDS);

    }

    @Override
    protected void onClose() {
    }

}
