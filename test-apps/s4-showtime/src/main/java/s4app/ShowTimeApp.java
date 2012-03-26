package s4app;

import org.apache.s4.core.App;

public class ShowTimeApp extends App {

    private ShowPE showPE;

    @Override
    protected void onStart() {
        System.out.println("Starting ShowTimeApp...");
        showPE.getInstanceForKey("single");
    }

    @Override
    protected void onInit() {
        System.out.println("Initing ShowTimeApp...");

        showPE = new ShowPE(this);

        /* This stream will receive events from another app. */
        createStream("clockStream", showPE);
    }

    @Override
    protected void onClose() {
        System.out.println("Closing ShowTimeApp...");
    }
}
