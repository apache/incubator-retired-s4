package org.apache.s4.comm.util;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.EventMessage;
import org.apache.s4.base.Listener;
import org.apache.s4.base.SerializerDeserializer;
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
    public SendThread sendThread;
    public ReceiveThread receiveThread;

    private final int numRetries;
    private final int retryDelayMs;
    private int numMessages;
    private int partitionId;
    private ProtocolTestUtil ptu;

    @Inject
    SerializerDeserializer serDeser;

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
        // this.messagesExpected = numMessages * this.emitter.getPartitionCount();

        this.sendThread = new SendThread();
        this.receiveThread = new ReceiveThread();
    }

    public void setProtocolTestUtil(ProtocolTestUtil ptu) {
        this.ptu = ptu;
        this.ptu.expectedMessages[partitionId] = numMessages * this.emitter.getPartitionCount();
    }

    public class SendThread extends Thread {
        public int[] sendCounts = new int[emitter.getPartitionCount()];

        public SendThread() {
            super("SendThread-" + partitionId);
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < numMessages; i++) {
                    for (int partition = 0; partition < emitter.getPartitionCount(); partition++) {
                        EventMessage message = new EventMessage("app1", "stream1",
                                new String(partitionId + " " + i).getBytes());
                        for (int retries = 0; retries < numRetries; retries++) {
                            if (emitter.send(partition, message)) {
                                sendCounts[partition]++;
                                break;
                            }
                            Thread.sleep(retryDelayMs);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            for (int i = 0; i < sendCounts.length; i++) {
                if (sendCounts[i] < numMessages) {
                    ptu.decreaseExpectedMessages(i, (numMessages - sendCounts[i]));
                }
            }

            logger.debug("Exiting");
        }
    }

    public class ReceiveThread extends Thread {
        protected int messagesReceived = 0;
        private Hashtable<Integer, List<Integer>> receivedMessages;

        ReceiveThread() {
            super("ReceiveThread-" + partitionId);
            receivedMessages = new Hashtable<Integer, List<Integer>>();
        }

        @Override
        public void run() {
            while (messagesReceived < ptu.expectedMessages[partitionId]) {
                byte[] message = listener.recv();
                if (message == null) {
                    logger.error("ReceiveThread {}: received a null message", partitionId);
                    break;
                }

                EventMessage deserialized = (EventMessage) serDeser.deserialize(message);
                // process and store the message
                String msgString = new String(deserialized.getSerializedEvent());
                String[] msgTokens = msgString.split(" ");
                Integer senderPartition = Integer.parseInt(msgTokens[0]);
                Integer receivedMsg = Integer.parseInt(msgTokens[1]);

                if (!receivedMessages.containsKey(senderPartition)) {
                    receivedMessages.put(senderPartition, new ArrayList<Integer>());
                }

                List<Integer> messagesList = receivedMessages.get(senderPartition);

                if (messagesList.contains(receivedMsg)) {
                    messagesList.remove(receivedMsg);
                } else {
                    messagesReceived++;
                }
                messagesList.add(receivedMsg);
            }

            logger.debug("Exiting");
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
            if (messagesReceived < ptu.expectedMessages[partitionId]) {
                printCounts();
                return false;
            } else
                return true;
        }

        public void printCounts() {
            logger.debug("ReceiveThread {}: Messages not received = {}", partitionId,
                    (ptu.expectedMessages[partitionId] - messagesReceived));
            int counts[] = new int[emitter.getPartitionCount()];
            for (Integer sender : receivedMessages.keySet()) {
                counts[sender] = receivedMessages.get(sender).size();
            }

            logger.debug("ReceiveThread {}: recvdCounts: {}", partitionId, counts);
        }

        public int moreMessages() {
            return (int) (ptu.expectedMessages[partitionId] - messagesReceived);
        }
    }
}
