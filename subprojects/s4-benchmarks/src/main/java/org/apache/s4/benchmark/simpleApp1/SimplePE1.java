package org.apache.s4.benchmark.simpleApp1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.atomic.AtomicLong;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.core.ProcessingElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

public class SimplePE1 extends ProcessingElement {

    private static Logger logger = LoggerFactory.getLogger(SimplePE1.class);

    private long warmupIterations = -1;
    boolean warmedUp = false;
    private long testIterations = -1;
    AtomicLong counter = new AtomicLong();
    BigDecimal rate;
    long lastTime = -1;

    public void setWarmupIterations(long warmupIterations) {
        this.warmupIterations = warmupIterations;
    }

    public void setTestIterations(long testIterations) {
        this.testIterations = testIterations;
    }

    public void onEvent(Event event) {
        counter.incrementAndGet();

        if (lastTime == -1) {
            lastTime = System.currentTimeMillis();
        } else {
            if ((System.currentTimeMillis() - lastTime) > 1000) {
                rate = new BigDecimal(counter.get()).divide(new BigDecimal(System.currentTimeMillis() - lastTime),
                        MathContext.DECIMAL64).multiply(new BigDecimal(1000));

                counter.set(0);
                lastTime = System.currentTimeMillis();
            }
        }

        Long value = event.get("value", Long.class);
        // logger.info("reached value {}", value);
        if (!warmedUp && (value == -1)) {
            logger.info("**** Warmed up");
            addSequentialNode("/warmup");
            warmedUp = true;

        } else if (value == (-2)) {
            logger.info("******* finished **************");

            addSequentialNode("/test");
            System.exit(0);
            logger.info("SADFASFDASFDASFDASFDASDFASDFASDF**************");

        }

    }

    private void addSequentialNode(String parent) {
        ZkClient zkClient = new ZkClient(((SimpleApp) getApp()).getZkString());
        zkClient.createPersistentSequential(parent + "/done", new byte[0]);
        zkClient.close();
    }

    @Override
    protected void onCreate() {
        Metrics.newGauge(SimplePE1.class, "simplePE1", new Gauge<BigDecimal>() {

            @Override
            public BigDecimal value() {
                return rate;
            }
        });
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub

    }

}
