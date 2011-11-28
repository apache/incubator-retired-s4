package org.apache.s4.comm;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/*
 * Test class to test communication protocols. As comm-layer connections need to be
 *  made including acquiring locks, the test is declared abstract and needs to be 
 *  extended with appropriate protocols.
 * 
 * At a high-level, the test accomplishes the following:
 * <ul>
 * <li> Create Send and Receive Threads </li>
 * <li> SendThread sends out a pre-defined number of messages to all the partitions </li>
 * <li> ReceiveThread receives all/most of these messages </li>
 * <li> To avoid the receiveThread waiting for ever, it spawns a TimerThread that would 
 * interrupt after a pre-defined but long enough interval </li>
 * <li> The receive thread reports on number of messages received and dropped </li>
 * </ul>
 * 
 */
public abstract class SimpleDeliveryTest {
    protected CommWrapper sdt;
    protected String lockdir;

    static class CommWrapper {
        private static final int MESSAGE_COUNT = 200;
        private static final int TIMER_THREAD_COUNT = 100;

        private final Emitter emitter;
        private final Listener listener;
        private final int interval;

        public Thread sendThread, receiveThread;
        private final int messagesExpected;
        private int messagesReceived = 0;

        @Inject
        public CommWrapper(@Named("emitter.send.interval") int interval, Emitter emitter, Listener listener) {
            this.emitter = emitter;
            this.listener = listener;
            this.interval = interval;
            this.messagesExpected = MESSAGE_COUNT * this.emitter.getPartitionCount();

            this.sendThread = new SendThread();
            this.receiveThread = new ReceiveThread();
        }

        public boolean moreMessages() {
            return ((messagesExpected - messagesReceived) > 0);
        }

        class SendThread extends Thread {
            @Override
            public void run() {
                try {
                    for (int partition = 0; partition < emitter.getPartitionCount(); partition++) {
                        for (int i = 0; i < MESSAGE_COUNT; i++) {
                            byte[] message = (new String("message-" + i)).getBytes();
                            emitter.send(partition, message);
                            Thread.sleep(interval);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            }
        }

        /*
         * TimerThread - interrupts the passed thread, after specified time-interval.
         */
        class TimerThread extends Thread {
            private final Thread watchThread;
            private Integer sleepCounter;

            TimerThread(Thread watchThread) {
                this.watchThread = watchThread;
                this.sleepCounter = new Integer(TIMER_THREAD_COUNT);
            }

            public void resetSleepCounter() {
                synchronized (this.sleepCounter) {
                    this.sleepCounter = TIMER_THREAD_COUNT;
                }
            }

            public void clearSleepCounter() {
                synchronized (this.sleepCounter) {
                    this.sleepCounter = 0;
                }
            }

            private int getCounter() {
                synchronized (this.sleepCounter) {
                    return this.sleepCounter--;
                }
            }

            @Override
            public void run() {
                try {
                    while (getCounter() > 0) {
                        Thread.sleep(interval);
                    }
                    watchThread.interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        class ReceiveThread extends Thread {
            @Override
            public void run() {

                // start the timer thread to interrupt if blocked for too long
                TimerThread timer = new TimerThread(this);
                timer.start();
                while (messagesReceived < messagesExpected) {
                    byte[] message = listener.recv();
                    timer.resetSleepCounter();
                    if (message != null)
                        messagesReceived++;
                    else
                        break;
                }
                timer.clearSleepCounter();
            }
        }
    }

    /*
     * All tests extending this class need to implement this method
     */
    @Before
    public abstract void setup();

    /**
     * Tests the protocol. If all components function without throwing exceptions, the test passes. The test also
     * reports the loss of messages, if any.
     * 
     * @throws InterruptedException
     */
    @Test
    public void testCommLayerProtocol() throws InterruptedException {
        try {
            // start send and receive threads
            sdt.sendThread.start();
            sdt.receiveThread.start();

            // wait for them to finish
            sdt.sendThread.join();
            sdt.receiveThread.join();

            Assert.assertTrue("Guaranteed message delivery", !sdt.moreMessages());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail("The comm protocol has failed basic functionality test");
        }

        Assert.assertTrue("The comm protocol seems to be working crash-free", true);

        System.out.println("Done");
    }
}
