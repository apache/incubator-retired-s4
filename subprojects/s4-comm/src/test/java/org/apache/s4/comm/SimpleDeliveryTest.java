package org.apache.s4.comm;

import org.apache.s4.base.Emitter;
import org.apache.s4.base.Listener;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.name.Named;

import junit.framework.Assert;
import junit.framework.TestCase;

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
public class SimpleDeliveryTest extends TestCase {

	class CommWrapper {
		final private static int messageCount = 200;
		final private static int timerThreadCount = 100;

		final private Emitter emitter;
		final private Listener listener;
		final private int interval;

		public Thread sendThread, receiveThread;
		private int messagesExpected;
		private int messagesReceived = 0;

		@Inject
		public CommWrapper(@Named("emitter.send.interval") int interval,
				Emitter emitter, Listener listener) {
			this.emitter = emitter;
			this.listener = listener;
			this.interval = interval;
			this.messagesExpected = messageCount
					* this.emitter.getPartitionCount();

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
					for (int partition = 0; partition < emitter
							.getPartitionCount(); partition++) {
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

	/**
	 * test1() tests the UDP protocol. If all components function without
	 * throwing exceptions, the test passes. As UDP doesn't guarantee message
	 * delivery, the number of messages received doesn't come into play to
	 * determine if it passes the test.
	 * 
	 * 
	 * @throws InterruptedException
	 */
	public void test1() throws InterruptedException {
		System.out.println("Testing UDP");

		Injector injector = Guice.createInjector(new UDPTestModule());
		try {
			CommWrapper sdt = injector.getInstance(CommWrapper.class);

			// start send and receive threads
			sdt.sendThread.start();
			sdt.receiveThread.start();

			// wait for them to finish
			sdt.sendThread.join();
			sdt.receiveThread.join();

			// exit - system.exit is called here to revoke the lock file and
			// listener
			// sockets
		} catch (Exception e) {
			Assert.fail("UDP test has failed");
		}
		Assert.assertTrue("UDP test PASSED. Seems to work fine", true);

		System.out.println("Done");
	}

	/**
	 * test2() tests the Netty TCP protocol. If all components function without
	 * throwing exceptions, the test passes partially. As TCP guarantees message
	 * delivery, the test checks for that too.
	 * 
	 * @throws InterruptedException
	 */
	public void test2() throws InterruptedException {
		System.out.println("Testing Netty TCP");

		Injector injector = Guice.createInjector(new NettyTestModule());
		try {
			CommWrapper sdt = injector.getInstance(CommWrapper.class);

			// start send and receive threads
			sdt.sendThread.start();
			sdt.receiveThread.start();

			// wait for them to finish
			sdt.sendThread.join();
			sdt.receiveThread.join();

			Assert.assertTrue("Guaranteed message delivery",
					!sdt.moreMessages());
		} catch (Exception e) {
			Assert.fail("Netty test has failed basic functionality test");
		}

		Assert.assertTrue("Netty seems to be working crash-free", true);

		System.out.println("Done");
	}
}
