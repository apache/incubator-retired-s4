package org.apache.s4.comm;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.s4.base.Emitter;

import com.google.inject.Inject;
import com.google.inject.name.Named;

public class QueueingEmitter implements Emitter, Runnable {
	private Emitter emitter;
	private BlockingQueue<MessageHolder> queue;
	private long dropCount = 0;
	private volatile Thread thread;

	@Inject
	public QueueingEmitter(@Named("ll") Emitter emitter,
			@Named("comm.queue_emmiter_size") int queueSize) {
		this.emitter = emitter;
		queue = new LinkedBlockingQueue<MessageHolder>(queueSize);
	}

	public long getDropCount() {
		return dropCount;
	}

	public void start() {
		if (thread != null) {
			throw new IllegalStateException(
					"QueueingEmitter is already started");
		}
		thread = new Thread(this, "QueueingEmitter");
		thread.start();
	}

	public void stop() {
		if (thread == null) {
			throw new IllegalStateException(
					"QueueingEmitter is already stopped");
		}
		thread.interrupt();
		thread = null;
	}

	@Override
	public boolean send(int partitionId, byte[] message) {
		MessageHolder mh = new MessageHolder(partitionId, message);
		if (!queue.offer(mh)) {
			dropCount++;
			return true;
		} else {
			return false;
		}
	}

	public void run() {
		while (!Thread.interrupted()) {
			try {
				MessageHolder mh = queue.take();
				// System.out.println("QueueingEmitter: Sending message on low-level emitter");
				emitter.send(mh.getPartitionId(), mh.getMessage());
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
			}
		}
	}

	public int getPartitionCount() {
		return emitter.getPartitionCount();
	}

	class MessageHolder {
		private int partitionId;
		private byte[] message;

		public int getPartitionId() {
			return partitionId;
		}

		public void setPartitionId(int partitionId) {
			this.partitionId = partitionId;
		}

		public byte[] getMessage() {
			return message;
		}

		public void setMessage(byte[] message) {
			this.message = message;
		}

		public MessageHolder(int partitionId, byte[] message) {
			super();
			this.partitionId = partitionId;
			this.message = message;
		}
	}

}
