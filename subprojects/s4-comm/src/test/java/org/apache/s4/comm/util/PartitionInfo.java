package org.apache.s4.comm.util;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Test util for communication protocols.
 * 
 * <ul>
 * <li>The util defines Send and Receive Threads</li>
 * <li>SendThread sends out a pre-defined number of messages to all the partitions</li>
 * <li>ReceiveThread receives all/most of these messages</li>
 * <li>To avoid the receiveThread waiting for ever, it spawns a TimerThread that would interrupt after a pre-defined but
 * long enough interval</li>
 * </ul>
 * 
 */
public class PartitionInfo {
    private static final Logger logger = LoggerFactory.getLogger(PartitionInfo.class);
    public Emitter emitter;
    public Listener listener;
    private final int interval;
    private int numMessages;
    private int partitionId;

    public SendThread sendThread;
    public ReceiveThread receiveThread;
    private int messagesExpected;

    @Inject
    public PartitionInfo(Emitter emitter, Listener listener, @Named("emitter.send.interval") int interval,
            @Named("emitter.send.numMessages") int numMessages) {
        this.emitter = emitter;
        this.listener = listener;
        this.partitionId = this.listener.getPartitionId();
        logger.debug("# Partitions = {}; Current partition = {}", this.emitter.getPartitionCount(),
                this.listener.getPartitionId());

        this.interval = interval;
        this.numMessages = numMessages;
        this.messagesExpected = numMessages * this.emitter.getPartitionCount();

        this.sendThread = new SendThread();
        this.receiveThread = new ReceiveThread();
    }

    public class SendThread extends Thread {

        public SendThread() {
            super("SendThread");
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < numMessages; i++) {
                    for (int partition = 0; partition < emitter.getPartitionCount(); partition++) {
                        byte[] message = new String(partitionId + " " + i).getBytes();
                        if (!emitter.send(partition, message)) {
                            logger.debug("SendThread {}: Resending message to {}", partitionId, partition);
                            Thread.sleep(interval);
                            if (!emitter.send(partition, message)) {
                                throw new RuntimeException("failed to send message");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            logger.debug("SendThread {}: Exiting", partitionId);
        }
    }

    private static class TimelyInterrupt extends TimerTask {
        private final Thread watchThread;

        TimelyInterrupt(Thread watchThread) {
            this.watchThread = watchThread;
        }

        @Override
        public void run() {
            watchThread.interrupt();
        }
    }

    public class ReceiveThread extends Thread {
        private int sleepCount = 10;
        private int messagesReceived = 0;
        private boolean ordered = true;
        private int receivedMessages[];
        private Timer timer;

        ReceiveThread() {
            super("ReceiveThread");
            receivedMessages = new int[emitter.getPartitionCount()];
            for (int i = 0; i < receivedMessages.length; i++)
                receivedMessages[i] = -1;
            timer = new Timer();
        }

        @Override
        public void run() {
            timer.schedule(new TimelyInterrupt(this), 10 * interval);

            while (messagesReceived < messagesExpected) {
                byte[] message = listener.recv();
                if (message != null) {
                    messagesReceived++;
                    String msgString = new String(message);
                    String[] msgTokens = msgString.split(" ");
                    int senderPartition = Integer.parseInt(msgTokens[0]);
                    int receivedMsg = Integer.parseInt(msgTokens[1]);
                    if (receivedMsg < receivedMessages[senderPartition])
                        ordered = false;
                    receivedMessages[senderPartition] = receivedMsg;
                } else if (sleepCount-- > 0) {
                    continue;
                } else {
                    break;
                }
            }
            timer.cancel();
            timer.purge();

            logger.debug("ReceiveThread {}: Exiting with {} messages left", partitionId, moreMessages());
        }

        public boolean ordered() {
            return ordered;
        }

        public int moreMessages() {
            return (messagesExpected - messagesReceived);
        }
    }
}
