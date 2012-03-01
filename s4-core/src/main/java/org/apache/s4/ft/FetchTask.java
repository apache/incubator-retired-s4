package org.apache.s4.ft;

import static org.apache.s4.util.MetricsName.S4_CORE_METRICS;

import java.util.concurrent.Callable;

import org.apache.s4.util.MetricsName;

/**
 * Encapsulates a fetching operation.
 *
 */
public class FetchTask implements Callable<byte[]>{

	StateStorage stateStorage;
	SafeKeeper safeKeeper;
	SafeKeeperId safeKeeperId;

	public FetchTask(StateStorage stateStorage, SafeKeeper safeKeeper,
			SafeKeeperId safeKeeperId) {
		super();
		this.stateStorage = stateStorage;
		this.safeKeeper = safeKeeper;
		this.safeKeeperId = safeKeeperId;
	}

	@Override
	public byte[] call() throws Exception {
		try {
			byte[] result = stateStorage.fetchState(safeKeeperId);
			if (safeKeeper.monitor!=null) {
				safeKeeper.monitor.increment(MetricsName.checkpointing_fetching_success.toString(), 1, S4_CORE_METRICS.toString());
			}
			return result;
		} catch (Exception e) {
			if (safeKeeper.monitor!=null) {
				safeKeeper.monitor.increment(MetricsName.checkpointing_fetching_failed.toString(), 1, S4_CORE_METRICS.toString());
			}
			throw e;
		}
	}

}
