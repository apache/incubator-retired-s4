package org.apache.s4.comm.util;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

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
    private final int numRetries;
    private final int retryDelayMs;
    private int numMessages;
    private int partitionId;

    public SendThread sendThread;
    public ReceiveThread receiveThread;
    private int messagesExpected;

    @Inject
    public PartitionInfo(Emitter emitter, Listener listener, @Named("comm.retries") int retries,
            @Named("comm.retry_delay") int retryDelay, @Named("emitter.send.numMessages") int numMessages) {
        this.emitter = emitter;
        this.listener = listener;
        this.partitionId = this.listener.getPartitionId();
        logger.debug("# Partitions = {}; Current partition = {}", this.emitter.getPartitionCount(),
                this.listener.getPartitionId());

        this.numRetries = retries;
        this.retryDelayMs = retryDelay;
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
            logger.debug("SendThread {}: started", partitionId);
            try {
                for (int i = 0; i < numMessages; i++) {
                    for (int partition = 0; partition < emitter.getPartitionCount(); partition++) {
                        byte[] message = new String(partitionId + " " + i).getBytes();
                        for (int retries = 0; retries < numRetries; retries++) {
                            if (emitter.send(partition, message))
                                break;
                            logger.debug("SendThread {}: Resending message to {}", partitionId, partition);
                            Thread.sleep(retryDelayMs);
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

    public class ReceiveThread extends Thread {
        private int messagesReceived = 0;
        private Hashtable<Integer, List<Integer>> receivedMessages;

        ReceiveThread() {
            super("ReceiveThread");
            receivedMessages = new Hashtable<Integer, List<Integer>>();
        }

        @Override
        public void run() {
            logger.debug("ReceiveThread    {}: started", partitionId);
            while (messagesReceived < messagesExpected) {
                byte[] message = listener.recv();
                if (message == null) {
                    logger.error("ReceiveThread {}: received a null message", partitionId);
                    break;
                }

                // process and store the message
                String msgString = new String(message);
                String[] msgTokens = msgString.split(" ");
                Integer senderPartition = Integer.parseInt(msgTokens[0]);
                Integer receivedMsg = Integer.parseInt(msgTokens[1]);

                if (!receivedMessages.containsKey(senderPartition)) {
                    receivedMessages.put(senderPartition, new ArrayList<Integer>());
                }

                List<Integer> messagesList = receivedMessages.get(senderPartition);

                if (messagesList.contains(receivedMsg)) {
                    // logger.debug("ReceiveThread {}: Already received message - {}", partitionId, msgString);
                    messagesList.remove(receivedMsg);
                } else {
                    messagesReceived++;
                }
                messagesList.add(receivedMsg);
            }

            logger.debug("ReceiveThread {}: Exiting with {} messages left", partitionId, moreMessages());
        }

        public boolean orderedDelivery() {
            for (List<Integer> messagesList : receivedMessages.values()) {
                int lastMsg = -1;
                for (Integer msg : messagesList) {
                    if (msg <= lastMsg) {
                        return false;
                    }
                }
            }
            return true;
        }

        public boolean messageDelivery() {
            for (List<Integer> messagesList : receivedMessages.values()) {
                if (messagesList.size() < numMessages) {
                    printRecvdCounts();
                    return false;
                }
            }
            return true;
        }

        public void printRecvdCounts() {
            int recvdCount[] = new int[emitter.getPartitionCount()];
            for (Integer sender : receivedMessages.keySet()) {
                recvdCount[sender] = receivedMessages.get(sender).size();
            }

            logger.debug("ReceiveThread {}: recvdCounts: {}", partitionId, recvdCount);
        }

        public int moreMessages() {
            return (messagesExpected - messagesReceived);
        }
    }
}
