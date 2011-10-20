package org.apache.s4.comm;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

/*
 * Test class to test communication protocols.
 * 
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
public class SimpleDeliveryTest {
	final private static int messageCount = 200;
	final private static int timerThreadCount = 100;

	final private Emitter emitter;
	final private Listener listener;
	final private int interval;
	
	public Thread sendThread, receiveThread;
	private int totalMessagesReceived;

	@Inject
	public SimpleDeliveryTest(@Named("emitter.send.interval") int interval,
			Emitter emitter, Listener listener) {
		this.emitter = emitter;
		this.listener = listener;
		this.interval = interval;
		this.totalMessagesReceived = messageCount
				* this.emitter.getPartitionCount();

		this.sendThread = new SendThread();
		this.receiveThread = new ReceiveThread();
	}

	class SendThread extends Thread {
		@Override
		public void run() {
			try {
				for (int partition = 0; partition < emitter.getPartitionCount(); partition++) {
					for (int i = 0; i < messageCount; i++) {
						byte[] message = (new String("message-" + i))
								.getBytes();
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
	 * TimerThread - interrupts the passed thread, after specified
	 * time-interval.
	 */
	class TimerThread extends Thread {
		private Thread watchThread;
		private Integer sleepCounter;

		TimerThread(Thread watchThread) {
			this.watchThread = watchThread;
			this.sleepCounter = new Integer(timerThreadCount);
		}

		public void resetSleepCounter() {
			synchronized (this.sleepCounter) {
				this.sleepCounter = timerThreadCount;
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
			int count = 0;

			// start the timer thread to interrupt if blocked for too long
			TimerThread timer = new TimerThread(this);
			timer.start();
			while (count < totalMessagesReceived) {
				byte[] message = listener.recv();
				timer.resetSleepCounter();
				if (message != null)
					count++;
				else
					break;
			}
			timer.clearSleepCounter();
			System.out.println("# Messages received = " + count
					+ ";\t # Messages lost = "
					+ (totalMessagesReceived - count));
		}
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		Injector injector = Guice.createInjector(new TestModule());
		SimpleDeliveryTest sdt = injector.getInstance(SimpleDeliveryTest.class);

		// start send and receive threads
		sdt.sendThread.start();
		sdt.receiveThread.start();

		// wait for them to finish
		sdt.sendThread.join();
		sdt.receiveThread.join();

		// exit - system.exit is called here to revoke the lock file and listner
		// sockets
		System.out.println("Done");
		System.exit(0);
	}
}
