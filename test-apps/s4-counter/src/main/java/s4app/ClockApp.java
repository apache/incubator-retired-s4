package s4app;

import java.util.concurrent.TimeUnit;

import org.apache.s4.core.App;
import org.apache.s4.core.EventSource;
import org.apache.s4.core.Streamable;

public class ClockApp extends App {

    private EventSource eventSource;
    private ClockPE clockPE;

    @Override
    protected void onStart() {
        System.out.println("Starting CounterApp...");
        clockPE.getInstanceForKey("single");
    }

    // generic array due to varargs generates a warning.
    @Override
    protected void onInit() {
        System.out.println("Initing CounterApp...");

        clockPE = new ClockPE(this);
        clockPE.setTimerInterval(1, TimeUnit.SECONDS);

        eventSource = new EventSource(this, "clockStream");
        clockPE.setStreams((Streamable) eventSource);
    }

    @Override
    protected void onClose() {
        System.out.println("Closing CounterApp...");
        eventSource.close();
    }

}
