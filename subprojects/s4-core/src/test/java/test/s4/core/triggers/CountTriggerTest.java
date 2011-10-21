package test.s4.core.triggers;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

public class CountTriggerTest extends TriggerTest {
    
    @Test
    public void testEventCountBasedTrigger() throws Exception {
        triggerType = TriggerType.COUNT_BASED;
        Assert.assertTrue(createTriggerAppAndSendEvent().await(5, TimeUnit.SECONDS));
    }

}
