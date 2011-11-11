package org.apache.s4.deploy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.s4.base.Event;
import org.apache.s4.core.Stream;

public class SocketAdapter {

    static ServerSocket serverSocket;

    /**
     * Listens to incoming sentence and forwards them to a sentence Stream. Each sentence is sent through a new socket
     * connection
     * 
     * @param stream
     * @throws IOException
     */
    public SocketAdapter(final Stream<Event> stream) throws IOException {
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
                        Event event = new Event();
                        event.put("line", String.class, line);
                        stream.put(event);
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

    public void close() {
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

}
