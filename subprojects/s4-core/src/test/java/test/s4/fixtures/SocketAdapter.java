package test.s4.fixtures;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.s4.core.Stream;

import test.s4.wordcount.KeyValueEvent;
import test.s4.wordcount.StringEvent;

public class SocketAdapter<T extends StringEvent> {

    static ServerSocket serverSocket;

    /**
     * Listens to incoming sentence and forwards them to a sentence Stream.
     * Each sentence is sent through a new socket connection
     * 
     * @param stream
     * @throws IOException
     */
    public SocketAdapter(final Stream<T> stream, final StringEventFactory<T> stringEventFactory) throws IOException {
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                serverSocket = null;
                Socket connectedSocket;
                BufferedReader in = null;
                try {
                    serverSocket = new ServerSocket(12000);
                    while (true) {
                        connectedSocket = serverSocket.accept();
                        in = new BufferedReader(new InputStreamReader(connectedSocket.getInputStream()));

                        String line = in.readLine();
                        System.out.println("read: " + line);
                        stream.put(stringEventFactory.create(line)) ;
                        connectedSocket.close();
                    }

                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    if (serverSocket != null) {
                        try {
                            serverSocket.close();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        });
        t.start();

    }
    
    public void close()  {
        if(serverSocket !=null) {
            try {
                serverSocket.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    interface StringEventFactory<T> {
        T create(String string);
    }
    
    public static class SentenceEventFactory implements StringEventFactory<StringEvent> {

        @Override
        public StringEvent create(String string) {
            return new StringEvent(string);
        }
        
    }
    
    public static class KeyValueEventFactory implements StringEventFactory<KeyValueEvent> {

        @Override
        public KeyValueEvent create(String string) {
            return new KeyValueEvent(string);
        }
        
    }

    
}
