package test.s4.core.triggers;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;


public class TimeTriggerTest extends TriggerTest {

    @Test
    public void testTimeBasedTrigger() throws Exception {
        triggerType = TriggerType.TIME_BASED;
        Assert.assertTrue(createTriggerAppAndSendEvent().await(5, TimeUnit.SECONDS));

    }

    
}
