package io.s4.wordcount;

import io.s4.TestUtils;
import io.s4.core.App;
import io.s4.core.ProcessingElement;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class WordClassifierPE extends ProcessingElement implements Watcher {

    TreeMap<String, Integer> counts = new TreeMap<String, Integer>();
    private int counter;
    transient private ZooKeeper zk;

    private WordClassifierPE () {}

    public WordClassifierPE(App app) {
        super(app);
    }
    
    public void onEvent(WordCountEvent event) {
        try {
            WordCountEvent wcEvent = event;
            if (zk == null) {
                try {
                    zk = new ZooKeeper("localhost:21810", 4000, this);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            System.out.println("seen: " + wcEvent.getWord() + "/" + wcEvent.getCount());

            if (!counts.containsKey(wcEvent.getWord())
                    || (counts.containsKey(wcEvent.getWord()) && counts.get(wcEvent.getWord()).compareTo(
                            wcEvent.getCount()) < 0)) {
                // this is because wcEvent events arrive unordered
                counts.put(wcEvent.getWord(), wcEvent.getCount());
            }
            ++counter;
            if (counter == WordCountTest.TOTAL_WORDS) {
                File results = new File(TestUtils.DEFAULT_TEST_OUTPUT_DIR + File.separator + "wordcount");
                if (results.exists()) {
                    if (!results.delete()) {
                        throw new RuntimeException("cannot delete results file");
                    }
                }
                Set<Entry<String, Integer>> entrySet = counts.entrySet();
                StringBuilder sb = new StringBuilder();
                for (Entry<String, Integer> entry : entrySet) {
                    sb.append(entry.getKey() + "=" + entry.getValue() + ";");
                }
                TestUtils.writeStringToFile(sb.toString(), results);

                zk.create("/textProcessed", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } else {
                // NOTE: this will fail if we did not recover the latest
                // counter,
                // because there is already a counter with this number in
                // zookeeper
                zk.create("/classifierIteration_" + counter, new byte[counter], Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
                Logger.getLogger("s4-ft").debug("wrote classifier iteration ["+counter+"]");
                System.out.println("wrote classifier iteration ["+counter+"]");
                // check if we are allowed to continue
                if (null == zk.exists("/continue_" + counter, null)) {
                    CountDownLatch latch = new CountDownLatch(1);
                    TestUtils.watchAndSignalCreation("/continue_" + counter, latch, zk);
                    latch.await();
                } else {
                    zk.delete("/continue_" + counter, -1);
                    System.out.println("");
                }
            }

        } catch (Exception e) {
            // TODO should propagate some exceptions
            e.printStackTrace();
        }

    }

    @Override
    public void process(WatchedEvent event) {
        // TODO Auto-generated method stub
 
    }

    @Override
    protected void onCreate() {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected void onRemove() {
        // TODO Auto-generated method stub
        
    }

}
