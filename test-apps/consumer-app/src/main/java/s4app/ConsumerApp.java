package s4app;

import org.apache.s4.core.App;

public class ConsumerApp extends App {

    private ConsumerPE consumerPE;

    @Override
    protected void onStart() {
        System.out.println("Starting ShowTimeApp...");
    }

    @Override
    protected void onInit() {
        System.out.println("Initing ShowTimeApp...");

        ConsumerPE consumerPE = createPE(ConsumerPE.class, "consumer");
        consumerPE.setSingleton(true);

        /* This stream will receive events from another app. */
        createInputStream("tickStream", consumerPE);
    }

    @Override
    protected void onClose() {
        System.out.println("Closing ShowTimeApp...");
    }
}
