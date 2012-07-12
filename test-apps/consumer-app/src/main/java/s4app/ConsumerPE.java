package s4app;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.core.App;
import org.apache.s4.core.ProcessingElement;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerPE extends ProcessingElement {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerPE.class);
    long eventCount = 0;

    public ConsumerPE(App app) {
        super(app);
    }

    public void onEvent(Event event) {
        eventCount++;
        logger.trace(
                "Received event with tick {} and time {} for event # {}",
                new String[] { String.valueOf(event.get("tick", Long.class)), String.valueOf(event.getTime()),
                        String.valueOf(eventCount) });
        if (eventCount == 1000) {
            logger.info("Just reached 1000 events");
            ZkClient zkClient = new ZkClient("localhost:2181");
            zkClient.create("/1000TicksReceived", new byte[0], CreateMode.PERSISTENT);
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
