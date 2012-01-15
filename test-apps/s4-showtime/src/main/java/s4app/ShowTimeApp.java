package s4app;

import org.apache.s4.core.App;

public class ShowTimeApp extends App {

    private ShowPE showPE;

    @Override
    protected void start() {
        System.out.println("Starting ShowTimeApp...");
        showPE.getInstanceForKey("single");
    }

    @Override
    protected void init() {
        System.out.println("Initing ShowTimeApp...");

        showPE = new ShowPE(this);

        /* This stream will receive events from another app. */
        createStream("I need the time.", showPE);
    }

    @Override
    protected void close() {
        System.out.println("Closing ShowTimeApp...");
    }

    @Override
    protected void onStart() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }
}
