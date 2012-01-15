package s4app;

import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShowPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(ShowPE.class);

    public ShowPE(App app) {
        super(app);
    }

    public void onEvent(Event event) {

        logger.info("Received event with tick {} and time {}.", event.get("tick", Long.class), event.getTime());

    }

    @Override
    protected void onRemove() {

    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub

    }
}
