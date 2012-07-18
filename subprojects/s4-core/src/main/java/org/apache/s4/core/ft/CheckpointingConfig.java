package org.apache.s4.core.ft;

import java.util.concurrent.TimeUnit;

/**
 * Checkpointing configuration: event count based vs time interval, frequency. User the {@link Builder} class to build
 * instances.
 *
 */
public class CheckpointingConfig {

    /**
     * Identifies the kind of checkpointing: time based, event count, or no checkpointing
     *
     */
    public static enum CheckpointingMode {
        TIME, EVENT_COUNT, NONE
    }

    public final CheckpointingMode mode;
    public final int frequency;
    public final TimeUnit timeUnit;

    private CheckpointingConfig(CheckpointingMode mode, int frequency, TimeUnit timeUnit) {
        this.mode = mode;
        this.frequency = frequency;
        this.timeUnit = timeUnit;
    }

    public static class Builder {
        private CheckpointingMode mode;
        private int frequency;
        private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

        public Builder(CheckpointingMode mode) {
            this.mode = mode;
        }

        public Builder frequency(int frequency) {
            this.frequency = frequency;
            return this;
        }

        public Builder timeUnit(TimeUnit timeUnit) {
            this.timeUnit = timeUnit;
            return this;
        }

        public CheckpointingConfig build() {
            return new CheckpointingConfig(mode, frequency, timeUnit);
        }

    }

}
