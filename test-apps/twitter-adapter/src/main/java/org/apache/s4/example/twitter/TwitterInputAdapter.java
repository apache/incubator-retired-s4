package org.apache.s4.example.twitter;

import java.net.ServerSocket;
import java.util.concurrent.LinkedBlockingQueue;

import org.I0Itec.zkclient.ZkClient;
import org.apache.s4.base.Event;
import org.apache.s4.core.adapter.Adapter;
import org.apache.s4.core.adapter.RemoteStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterInputAdapter extends Adapter {

    private ZkClient zkClient;
    private static Logger logger = LoggerFactory.getLogger(TwitterInputAdapter.class);
    private String urlString = "https://stream.twitter.com/1/statuses/sample.json";

    public TwitterInputAdapter() {
    }

    private LinkedBlockingQueue<Status> messageQueue = new LinkedBlockingQueue<Status>();

    protected ServerSocket serverSocket;

    private Thread t;

    private int messageCount;

    private RemoteStream remoteStream;

    @Override
    protected void onClose() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void onInit() {
        remoteStream = createRemoteStream("RawStatus");
        t = new Thread(new Dequeuer());
    }

    public void connectAndRead() throws Exception {

        TwitterStream twitterStream = TwitterStreamFactory.getSingleton();
        StatusListener statusListener = new StatusListener() {

            @Override
            public void onException(Exception ex) {
                logger.error("error", ex);
            }

            @Override
            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                logger.error("error");
            }

            @Override
            public void onStatus(Status status) {
                messageQueue.add(status);

            }

            @Override
            public void onScrubGeo(long userId, long upToStatusId) {
                logger.error("error");
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                logger.error("error");
            }
        };
        twitterStream.addListener(statusListener);
        twitterStream.sample();

    }

    @Override
    protected void onStart() {
        try {
            t.start();
            connectAndRead();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    class Dequeuer implements Runnable {

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                try {
                    Status status = messageQueue.take();
                    Event event = new Event();
                    event.put("statusText", String.class, status.getText());
                    remoteStream.put(event);
                } catch (Exception e) {

                }
            }

        }
    }
}
