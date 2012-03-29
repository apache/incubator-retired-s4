package org.apache.s4.comm;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Listener;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/*
 * Test util for communication protocols.
 *
 * <ul>
 * <li> The util defines Send and Receive Threads </li>
 * <li> SendThread sends out a pre-defined number of messages to all the partitions </li>
 * <li> ReceiveThread receives all/most of these messages </li>
 * <li> To avoid the receiveThread waiting for ever, it spawns a TimerThread that would
 * interrupt after a pre-defined but long enough interval </li>
 * </ul>
 *
 */
public class DeliveryTestUtil {

    private final Emitter emitter;
    private final Listener listener;
    private final int interval;
    private int numMessages;
    private int sleepCount;

    // public Thread sendThread, receiveThread;
    private final int messagesExpected;

    @Inject
    public DeliveryTestUtil(Emitter emitter, Listener listener, @Named("emitter.send.interval") int interval,
            @Named("emitter.send.numMessages") int numMessages, @Named("listener.recv.sleepCount") int sleepCount) {
        this.emitter = emitter;
        this.listener = listener;
        this.interval = interval;
        this.numMessages = numMessages;
        this.sleepCount = sleepCount;
        this.messagesExpected = numMessages * this.emitter.getPartitionCount();

        // this.sendThread = new SendThread();
        // this.receiveThread = new ReceiveThread();
    }

    public class SendThread extends Thread {
        @Override
        public void run() {
            try {
                for (int partition = 0; partition < emitter.getPartitionCount(); partition++) {
                    for (int i = 0; i < numMessages; i++) {
                        byte[] message = (new String("message-" + i)).getBytes();
                        emitter.send(partition, new EventMessage(null, null, message));
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
            this.sleepCounter = new Integer(sleepCount);
        }

        public void resetSleepCounter() {
            synchronized (this.sleepCounter) {
                this.sleepCounter = sleepCount;
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
        private int messagesReceived = 0;

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

        private boolean moreMessages() {
            return ((messagesExpected - messagesReceived) > 0);
        }
    }

    public Thread newSendThread() {
        return new SendThread();
    }

    public Thread newReceiveThread() {
        return new ReceiveThread();
    }

    public boolean moreMessages(Thread recvThread) {
        return ((ReceiveThread) recvThread).moreMessages();
    }
}
