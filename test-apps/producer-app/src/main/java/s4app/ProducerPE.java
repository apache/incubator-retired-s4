package s4app;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.s4.core.Streamable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(ProducerPE.class);

    private Streamable[] targetStreams;
    private long tick = 0;

    public ProducerPE(App app) {
        super(app);
    }

    /**
     * @param targetStreams
     *            the {@link UserEvent} streams.
     */
    public void setStreams(Streamable... targetStreams) {
        this.targetStreams = targetStreams;
    }

    public void onTime() {
        if (tick < 1000) {
            Event event = new Event();
            event.put("tick", Long.class, tick++);

            logger.trace("Sending event with tick {} and time {}.", event.get("tick", Long.class), event.getTime());
            for (int i = 0; i < targetStreams.length; i++) {
                targetStreams[i].put(event);
            }
        }
    }

    @Override
    protected void onRemove() {

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }
}
